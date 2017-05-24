package water.rapids.ast.prims.reducers;

import hex.SplitFrame;
import org.junit.BeforeClass;
import org.junit.Test;
import water.DKV;
import water.Key;
import water.Scope;
import water.TestUtil;
import water.fvec.C8Chunk;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.rapids.Rapids;
import water.rapids.Val;

import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Test the AstTopN.java class
 */
public class AstTopNTest extends TestUtil {
    static Frame _train;    // store training data
    public Random _rand = new Random();

    @BeforeClass public static void setup() {   // randomly generate a frame here.
        stall_till_cloudsize(1);
    }

    //--------------------------------------------------------------------------------------------------------------------
    // Tests
    //--------------------------------------------------------------------------------------------------------------------
    /** Loading in a dataset containing data from -1000000 to 1000000 multiplied by 1.1 as the float column in column 1.
     * The other column (column 0) is a long data type with maximum data value at 2^63. */
    @Test public void TestTopBottomN() {
        Scope.enter();
        int[] checkPercent = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}; // complete test
        int numRuns = 4;
        int testPercent = 0;      // store test percentage
        Frame topLong = null, topFloat=null, bottomLong=null, bottomFloat=null;
        double tolerance=1e-12;

        // load in the datasets with the answers
        _train = parse_test_file(Key.make("topbottom"), "smalldata/jira/TopBottomN.csv.zip");
        topFloat = parse_test_file(Key.make("top20"), "smalldata/jira/Top20Per.csv.zip");
        topLong = topFloat.extractFrame(0,1);
        bottomFloat = parse_test_file(Key.make("bottom20"), "smalldata/jira/Bottom20Per.csv.zip");
        bottomLong = bottomFloat.extractFrame(0,1);

        Scope.track(_train);
        Scope.track(topLong);
        Scope.track(topFloat);
        Scope.track(bottomFloat);
        Scope.track(bottomLong);
        Chunk[] tempChunkT = new Chunk[1];
        Chunk[] tempChunkB = new Chunk[1];
        tempChunkT[0] = topLong.vec(0).chunkForChunkIdx(0);
        tempChunkB[0] = bottomLong.vec(0).chunkForChunkIdx(0);

        try {
            for (int index = 0; index < numRuns; index++) { // randomly choose 4 percentages to test
                testPercent = checkPercent[_rand.nextInt(checkPercent.length)];

                if (tempChunkT[0] instanceof C8Chunk)   // ToDo: Tomas, Long is not read in as long here?
                    testTopBottom(topLong, testPercent, 0, "0", 0);  // test top % Long
                else
                    testTopBottom(topLong, testPercent, 0, "0", tolerance);
                testTopBottom(topFloat, testPercent, 0, "1", tolerance);  // test top % Float
                if (tempChunkT[0] instanceof C8Chunk)
                    testTopBottom(bottomLong, testPercent, 1, "0", 0);  // test bottom % Long
                else
                    testTopBottom(bottomLong, testPercent, 1, "0", tolerance);  // test bottom % Long

                testTopBottom(bottomFloat, testPercent, 1, "1", tolerance);  // test bottom % Float
            }
        } finally {
            Scope.exit();
        }
    }

    public static void testTopBottom(Frame topBottom, int testPercent, int getBottom, String columnIndex,
                                     double tolerance) {
        Scope.enter();
        Frame topBN = null, topBL = null;
        try {
            String x = "(topn " + _train._key + " "+ columnIndex + " " +testPercent + " " + getBottom + ")";
            Val res = Rapids.exec(x);         // make the call to grab top/bottom N percent
            topBN = res.getFrame();            // get frame that contains top N elements
            Scope.track(topBN);
            topBL = topBN.extractFrame(1,2);
            Scope.track(topBL);
            checkTopBottomN(topBottom, topBL, tolerance);
        } finally {
            Scope.exit();
        }
    }
    /*
    Helper function to compare test frame result with correct answer
     */
    public static void checkTopBottomN(Frame answerF, Frame grabF, double tolerance) {
        Scope.enter();
        try {
            double nfrac = 1.0*grabF.numRows()/answerF.numRows();   // translate percentage to actual fraction
            SplitFrame sf = new SplitFrame(answerF, new double[]{nfrac, 1 - nfrac}, new Key[]{
                    Key.make("topN.hex"), Key.make("bottomN.hex")});
            // Invoke the job
            sf.exec().get();
            Key[] ksplits = sf._destination_frames;
            Frame topN = DKV.get(ksplits[0]).get();
            assertTrue(isIdenticalUpToRelTolerance(topN, grabF, tolerance));
            Scope.track(topN);
            Scope.track_generic(ksplits[1].get());
        } finally {
            Scope.exit();
        }
    }
}