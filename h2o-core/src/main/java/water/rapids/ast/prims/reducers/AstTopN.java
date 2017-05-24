package water.rapids.ast.prims.reducers;

import water.MRTask;
import water.fvec.C8Chunk;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.rapids.Env;
import water.rapids.ast.AstPrimitive;
import water.rapids.ast.AstRoot;
import water.rapids.vals.ValFrame;

import java.util.ArrayList;
import java.util.TreeMap;

import static java.lang.StrictMath.min;

public class AstTopN extends AstPrimitive {
  @Override
  public String[] args() {
    return new String[]{"frame", "col", "nPercent", "getBottomN"};
  }

  @Override
  public String str() {
    return "topn";
  }

  @Override
  public int nargs() {
    return 1+4;
  } // function name plus 4 arguments.

  @Override
  public String example() {
    return "(topn frame col nPercent getBottomN)";
  }

  @Override
  public String description() {
    return "Return the top N percent rows for a numerical column as a frame with two columns.  The first column " +
            "will contain the original row indices of the chosen values.  The second column contains the top N row" +
            "values.  If getBottomN is 1, we will return the bottom N percent.  If getBottomN is 0, we will return" +
            "the top N percent of rows";
  }

  @Override
  public ValFrame apply(Env env, Env.StackHelp stk, AstRoot[] asts) {
    Frame frOriginal = stk.track(asts[1].exec(env)).getFrame(); // get the 2nd argument and convert it to a Frame
    int colIndex = (int) stk.track(asts[2].exec(env)).getNum();     // column index of interest
    double nPercent = stk.track(asts[3].exec(env)).getNum();        //  top or bottom percentage of row to return
    int getBottomN = (int) stk.track(asts[4].exec(env)).getNum();   // 0, return top, 1 return bottom percentage
    int totColumns = frOriginal.numCols();
    long numRows = Math.round(nPercent*0.01*frOriginal.numRows()); // number of rows to return

    // check for valid input parameter values
    assert ((nPercent>0) && (nPercent<=100)); // make sure percent is between 0 and 100.
    assert (numRows >= 0);                    // make sure we have at least one row to extract
    assert ((colIndex >=0) && (colIndex < totColumns)); // valid column index specification
    assert ((getBottomN ==0) || (getBottomN==1));       // must be 0 or 1
    assert frOriginal.vec(colIndex).isNumeric();        // make sure we are dealing with numerical column only

    String[] finalColumnNames = {"Original_Row_Indices", frOriginal.name(colIndex)}; // set output frame names
    GrabTopN grabTask = new GrabTopN(finalColumnNames, numRows, (getBottomN==0));
    grabTask.doAll(frOriginal.vec(colIndex));
    return new ValFrame(grabTask._sortedOut);
  }

  /*
   Here is the plan:
   1. For each chunk (inside each map function of our MRTask), put all elements into a sorted heap with key as the
    column value to grab and original row indices (in an array to deal with duplicated values) as the value.  At
    the end of map, reduce the sorted heap to size N percent rows.
   2. Inside reduce, make sure you combine the heaps and again reduce the sorted heap back to size N percent rows.
   3. Inside the postGlobal, copy the heap (key, values).
   */
// E depends on column type: long or other numerics
  public  class GrabTopN<E extends Comparable<E>> extends MRTask<GrabTopN<E>>  {
    final String[] _columnName;   // name of column that we are grabbing top N for
    TreeMap _sortHeap;
    Frame _sortedOut;   // store the final result of sorting
    final long _rowSize;   // number of top or bottom rows to keep
    final boolean _increasing;  // sort with Top values first if true.
    boolean _csLong=false;      // chunk of interest is long

    private GrabTopN(String[] columnName, long rowSize, boolean increasing) {
      _columnName = columnName;
      _rowSize = rowSize;
      _increasing = increasing;
    }

    @Override public void map(Chunk cs) {
      _sortHeap = new TreeMap();
      _csLong = cs instanceof C8Chunk;
      Long startRow = cs.start();           // absolute row offset

      for (int rowIndex = 0; rowIndex < cs._len; rowIndex++) {  // stuff our chunks into hashmap
        long absRowIndex = rowIndex+startRow;
        if (!cs.isNA(rowIndex)) { // skip NAN values
          addOneValue(cs, rowIndex, absRowIndex, _sortHeap);
        }
      }

      // reduce heap size to about rowSize
      if (_sortHeap.size() > _rowSize) {  // chop down heap size to around _rowSize
        reduceHeapSize(_sortHeap, _rowSize);
      }
    }

    @Override public void reduce(GrabTopN<E> other) {
      this._sortHeap.putAll(other._sortHeap);

      if (this._sortHeap.size() > _rowSize) {
        reduceHeapSize(this._sortHeap, _rowSize); // shrink the heap size again.
      }
    }

    /*
    Copy the top/bottom N elements of the TreepMap to a frame as the final output.
     */
    @Override public void postGlobal() {  // copy the sorted heap into a vector and make a frame out of it.
      long rowCount = 0l; // count number of rows extracted from Heap and setting to final frame
      Vec[] xvecs = new Vec[2];   // final output frame will have two chunks, original row index, top/bottom values
      long actualRowOutput = min(_rowSize, _sortHeap.size()); // due to NAs, may not have enough rows to return
      for (int index = 0; index < xvecs.length; index++)
        xvecs[index] = Vec.makeZero(actualRowOutput);

      while (rowCount < actualRowOutput) {
        if (_sortHeap.size()>0) {   // copy top/bottom N values over to vecs.
          rowCount += addOneRow(xvecs, rowCount, actualRowOutput);
        }
      }
      _sortedOut = new Frame(_columnName, xvecs);
    }

    /*
    The heap size is reduced to contain the desiredSize number of nodes.
     */
    public void reduceHeapSize(TreeMap tmap, long desiredSize) {
      long numDelete = tmap.size()-desiredSize;
      for (long index=0; index<numDelete; index++) {
       if (_increasing)
          tmap.remove(tmap.firstKey());
        else
          tmap.remove(tmap.lastKey());
      }
    }

    /*
    This function will add one value to the sorted heap.  If the values are the same, the row indices are added to the
    value of the heap node as we use the values as the key.
     */
    public void addOneValue(Chunk cs, int rowIndex, long absRowIndex, TreeMap sortHeap) {
      if (_csLong) {  // long chunk
        long a = cs.at8(rowIndex);
        if (sortHeap.containsKey(a)) {
          ArrayList<Long> allRows = (ArrayList<Long>) sortHeap.get(a);
          allRows.add(absRowIndex);
          sortHeap.put(a, allRows);
        } else {
          ArrayList<Long> allRows = new ArrayList<Long>();
          allRows.add(absRowIndex);
          sortHeap.put(a, allRows);
        }
      } else {                      // other numeric chunk
        double a = cs.atd(rowIndex);
        if (sortHeap.containsKey(a)) {
          ArrayList<Long> allRows = (ArrayList<Long>) sortHeap.get(a);
          allRows.add(absRowIndex);
          sortHeap.put(a, allRows);
        } else {
          ArrayList<Long> allRows = new ArrayList<Long>();
          allRows.add(absRowIndex);
          sortHeap.put(a, allRows);
        }
      }
    }

    /*
    For every heap element, we will copy over the value as the original row indices and the key the top/bottom
    values into a Vec[] array of two columns.  First column is the original row index, and the second column
    contains the key which is the values we desire.
     */
    public long addOneRow(Vec[] xvecs, long rowCount, long absMaxSize) {
      ArrayList<Long> rowValues;
      E key;
      int rowIncreased = 0;

      key = _increasing?(E) _sortHeap.lastKey():(E) _sortHeap.firstKey();
/*      if (_increasing) {
        key = (E) _sortHeap.lastKey();
      } else {
        key = (E) _sortHeap.firstKey();
      }*/
      rowValues = (ArrayList<Long>) _sortHeap.get(key);
        _sortHeap.remove(key);
        for (int i = 0; i < rowValues.size(); i++) {
          xvecs[0].set(rowCount, rowValues.get(i));
          xvecs[1].set(rowCount, _csLong?(Long) key:(Double) key);
/*          if (_csLong)
            xvecs[1].set(rowCount, (Long) key);
          else
            xvecs[1].set(rowCount, (Double) key);*/

          rowCount++;
          rowIncreased++;
          if (rowCount >= absMaxSize)
            return rowIncreased;
        }
      return rowIncreased;
    }
  }

}
