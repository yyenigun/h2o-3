from __future__ import print_function
import sys
sys.path.insert(1,"../../")
import h2o
from tests import pyunit_utils
from random import randint
from h2o.utils.typechecks import assert_is_type
from h2o.frame import H2OFrame

def h2o_H2OFrame_top_bottomN():
    """
    PUBDEV-3624 Top or Bottom N test h2o.frame.H2OFrame.topN() and h2o.frame.H2OFrame.bottomN() functions.
    Given a H2O frame, a column index or column name, a double denoting percentages of top/bottom rows to 
    return, the topN will return a H2OFrame containing two columns, one will
    be the topN (or bottomN) values of the specified column.  The other column will record the row indices into
    the original frame of where the topN (bottomN) values come from.  This will let the users to grab those
    corresponding rows to do whatever they want with it.
    """
    dataFrame = h2o.import_file(pyunit_utils.locate("bigdata/laptop/jira/TopBottomNRep4.csv.zip"))
    topAnswer = h2o.import_file(pyunit_utils.locate("smalldata/jira/Top20Per.csv.zip"))
    bottomAnswer = h2o.import_file(pyunit_utils.locate("smalldata/jira/Bottom20Per.csv.zip"))
    nPercentages = [4,8,12,16]  # multiples of 4 since dataset is repeated 4 times.
    frameNames = dataFrame.names    # get data column names
    tolerance=1e-12

    nP = nPercentages[randint(0, len(nPercentages)-1)]  # pick a random percentage
    colIndex = randint(0, len(frameNames)-1)    # pick a random column
    newTopFrame = dataFrame.topN(frameNames[colIndex], nP)  # call topN with column names
    newTopFrameC = dataFrame.topN(colIndex, nP)             # call topN with same column index

    # the two return frames should be the same for this case, compare 1000 rows chosen randomly
    pyunit_utils.compare_frames(newTopFrame, newTopFrameC, 100, tol_numeric=tolerance)

    # compare one of the return frames with known answer
    compare_rep_frames(topAnswer, newTopFrame, tolerance)

    # test bottomN here
    nP = nPercentages[randint(0, len(nPercentages)-1)]  # pick a random percentage
    colIndex = randint(0, len(frameNames)-1)    # pick a random column
    newBottomFrame = dataFrame.bottomN(frameNames[colIndex], nP)  # call topN with column names
    newBottomFrameC = dataFrame.bottomN(colIndex, nP)             # call topN with same column index

    # the two return frames should be the same for this case
    pyunit_utils.compare_frames(newBottomFrame, newBottomFrameC, 100, tol_numeric=tolerance)
    # compare one of the return frames with known answer
    compare_rep_frames(bottomAnswer, newTopFrame, tolerance)


def compare_rep_frames(answerF, repFrame, tolerance):
    # actual answer is in second column of repFrame
    for ind in range(repFrame.nrow):
        assert abs(answerF[round(ind, 4), 0]-repFrame[ind, 1]) < tolerance, \
            "Expected {0}, Actual {1} .".format(answerF[0, round(ind,4)], repFrame[1,ind])

if __name__ == "__main__":
    pyunit_utils.standalone_test(h2o_H2OFrame_top_bottomN)
else:
    h2o_H2OFrame_top_bottomN()


