package hex.tree.xgboost;

import hex.ModelMetricsBinomial;
import hex.ModelMetricsMultinomial;
import hex.ModelMetricsRegression;
import hex.SplitFrame;
import hex.genmodel.utils.DistributionFamily;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import water.DKV;
import water.Key;
import water.Scope;
import water.TestUtil;
import water.fvec.Frame;
import water.util.Log;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;

import static water.util.FileUtils.locateFile;

public class XGBoostTest extends TestUtil {

  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  @BeforeClass
  public static void _checkBackend() { // NOTE: the `_` force execution of this check after setup
    Assume.assumeTrue("XGBoost was not loaded!\n"
                      + "H2O XGBoost needs binary compatible environment;"
                      + "Make sure that you have correct libraries installed"
                      + "and correctly configured LD_LIBRARY_PATH, especially"
                      + "make sure that CUDA libraries are available if you are running on GPU!",
                      hex.tree.xgboost.XGBoost.haveBackend());
  }

  static DMatrix[] getMatrices() throws XGBoostError {
    // load file from text file, also binary buffer generated by xgboost4j
    return new DMatrix[]{
        new DMatrix(locateFile("smalldata/xgboost/demo/data/agaricus.txt.train").getAbsolutePath()),
        new DMatrix(locateFile("smalldata/xgboost/demo/data/agaricus.txt.test").getAbsolutePath())
    };
  }
  static void saveDumpModel(File modelFile, String[] modelInfos) throws IOException {
    try{
      PrintWriter writer = new PrintWriter(modelFile, "UTF-8");
      for(int i = 0; i < modelInfos.length; ++ i) {
        writer.print("booster[" + i + "]:\n");
        writer.print(modelInfos[i]);
      }
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static boolean checkPredicts(float[][] fPredicts, float[][] sPredicts) {
    if (fPredicts.length != sPredicts.length) {
      return false;
    }

    for (int i = 0; i < fPredicts.length; i++) {
      if (!Arrays.equals(fPredicts[i], sPredicts[i])) {
        return false;
      }
    }

    return true;
  }

  @Test
  public void testMatrices() throws XGBoostError { getMatrices(); }

  @Test public void BasicModel() throws XGBoostError {
    // load file from text file, also binary buffer generated by xgboost4j
    DMatrix[] mat = getMatrices();
    DMatrix trainMat = mat[0];
    DMatrix testMat = mat[1];

    HashMap<String, Object> params = new HashMap<>();
    params.put("eta", 0.1);
    params.put("max_depth", 5);
    params.put("silent", 1);
    params.put("objective", "binary:logistic");

    HashMap<String, DMatrix> watches = new HashMap<>();
    watches.put("train", trainMat);
    watches.put("test",  testMat);

    Booster booster = XGBoost.train(trainMat, params, 10, watches, null, null);
    float[][] preds = booster.predict(testMat);
    for (int i=0;i<10;++i)
      Log.info(preds[i][0]);
  }

  @Test public void testScoring() throws XGBoostError {
    // load file from text file, also binary buffer generated by xgboost4j
    DMatrix[] mat = getMatrices();
    DMatrix trainMat = mat[0];
    DMatrix testMat = mat[1];

    HashMap<String, Object> params = new HashMap<>();
    params.put("eta", 0.1);
    params.put("max_depth", 5);
    params.put("silent", 1);
    params.put("objective", "reg:linear");

    HashMap<String, DMatrix> watches = new HashMap<>();
    watches.put("train", trainMat);
    watches.put("test",  testMat);

    Booster booster = XGBoost.train(trainMat, params, 10, watches, null, null);
    // slice some rows out and predict on those
    float[][] preds1 = booster.predict(trainMat.slice(new int[]{0}));
    float[][] preds2 = booster.predict(trainMat.slice(new int[]{1}));
    float[][] preds3 = booster.predict(trainMat.slice(new int[]{2}));
    float[][] preds4 = booster.predict(trainMat.slice(new int[]{0,1,2}));

    Assert.assertTrue(preds1.length==1);
    Assert.assertTrue(preds2.length==1);
    Assert.assertTrue(preds3.length==1);
    Assert.assertTrue(preds4.length==3);

    Assert.assertTrue(preds4[0][0]==preds1[0][0]);
    Assert.assertTrue(preds4[1][0]==preds2[0][0]);
    Assert.assertTrue(preds4[2][0]==preds3[0][0]);
    Assert.assertTrue(preds4[0][0]!=preds4[1][0]);
    Assert.assertTrue(preds4[0][0]!=preds4[2][0]);
  }

  @Test public void testScore0() throws XGBoostError {
    // trivial dataset with 3 rows and 2 columns
    // (4,5) -> 1
    // (3,1) -> 2
    // (2,3) -> 3
    DMatrix trainMat = new DMatrix(new float[]{4f,5f, 3f,1f, 2f,3f},3,2);
    trainMat.setLabel(new float[]{             1f,    2f,    3f       });

    HashMap<String, Object> params = new HashMap<>();
    params.put("eta", 1);
    params.put("max_depth", 5);
    params.put("silent", 1);
    params.put("objective", "reg:linear");

    HashMap<String, DMatrix> watches = new HashMap<>();
    watches.put("train", trainMat);

    Booster booster = XGBoost.train(trainMat, params, 10, watches, null, null);

    // check overfitting
    // (4,5) -> 1
    // (3,1) -> 2
    // (2,3) -> 3
    float[][] preds1 = booster.predict(new DMatrix(new float[]{4f,5f},1,2));
    float[][] preds2 = booster.predict(new DMatrix(new float[]{3f,1f},1,2));
    float[][] preds3 = booster.predict(new DMatrix(new float[]{2f,3f},1,2));

    Assert.assertTrue(preds1.length==1);
    Assert.assertTrue(preds2.length==1);
    Assert.assertTrue(preds3.length==1);

    Assert.assertTrue(Math.abs(preds1[0][0]-1) < 1e-2);
    Assert.assertTrue(Math.abs(preds2[0][0]-2) < 1e-2);
    Assert.assertTrue(Math.abs(preds3[0][0]-3) < 1e-2);
  }

  @Test
  public void saveLoadDataAndModel() throws XGBoostError, IOException {
    // load file from text file, also binary buffer generated by xgboost4j
    DMatrix[] mat = getMatrices();
    DMatrix trainMat = mat[0];
    DMatrix testMat = mat[1];

    HashMap<String, Object> params = new HashMap<>();
    params.put("eta", 0.1);
    params.put("max_depth", 5);
    params.put("silent", 1);
    params.put("objective", "binary:logistic");

    HashMap<String, DMatrix> watches = new HashMap<>();
    watches.put("train", trainMat);
    watches.put("test",  testMat);

    Booster booster = XGBoost.train(trainMat, params, 10, watches, null, null);

    float[][] predicts = booster.predict(testMat);

    //save model to modelPath
    File modelDir = java.nio.file.Files.createTempDirectory("xgboost-model").toFile();
    
    booster.saveModel(path(modelDir, "xgb.model"));

    //dump model with feature map
    String[] modelInfos = booster.getModelDump(locateFile("smalldata/xgboost/demo/data/featmap.txt").getAbsolutePath(), false);
    saveDumpModel(new File(modelDir, "dump.raw.txt"), modelInfos);

    //save dmatrix into binary buffer
    testMat.saveBinary(path(modelDir, "dtest.buffer"));

    //reload model and data
    Booster booster2 = XGBoost.loadModel(path(modelDir, "xgb.model"));
    DMatrix testMat2 = new DMatrix(path(modelDir, "dtest.buffer"));
    float[][] predicts2 = booster2.predict(testMat2);

    //check the two predicts
    System.out.println(checkPredicts(predicts, predicts2));

    //specify watchList
    HashMap<String, DMatrix> watches2 = new HashMap<>();
    watches2.put("train", trainMat);
    watches2.put("test", testMat2);
    Booster booster3 = XGBoost.train(trainMat, params, 10, watches2, null, null);
    float[][] predicts3 = booster3.predict(testMat2);

    //check predicts
    System.out.println(checkPredicts(predicts, predicts3));
  }

  private static String path(File parentDir, String fileName) {
    return new File(parentDir, fileName).getAbsolutePath();
  }

  @Test
  public void checkpoint() throws XGBoostError, IOException {
    // load file from text file, also binary buffer generated by xgboost4j
    DMatrix[] mat = getMatrices();
    DMatrix trainMat = mat[0];
    DMatrix testMat = mat[1];

    HashMap<String, Object> params = new HashMap<>();
    params.put("eta", 0.1);
    params.put("max_depth", 5);
    params.put("silent", 1);
    params.put("objective", "binary:logistic");

    HashMap<String, DMatrix> watches = new HashMap<>();
    watches.put("train", trainMat);

    Booster booster = XGBoost.train(trainMat, params, 0, watches, null, null);

    // Train for 10 iterations
    for (int i=0;i<10;++i) {
      booster.update(trainMat, i);
      float[][] preds = booster.predict(testMat);
      for (int j = 0; j < 10; ++j)
        Log.info(preds[j][0]);
    }
  }

  @Test
  public void WeatherBinary() {
    Frame tfr = null;
    Frame trainFrame = null;
    Frame testFrame = null;
    Frame preds = null;
    XGBoostModel model = null;
    Scope.enter();
    try {
      // Parse frame into H2O
      tfr = parse_test_file("./smalldata/junit/weather.csv");
      // define special columns
      String response = "RainTomorrow";
//      String weight = null;
//      String fold = null;
      Scope.track(tfr.replace(tfr.find(response), tfr.vecs()[tfr.find(response)].toCategoricalVec()));
      // remove columns correlated with the response
      tfr.remove("RISK_MM").remove();
      tfr.remove("EvapMM").remove();
      DKV.put(tfr);

      // split into train/test
      SplitFrame sf = new SplitFrame(tfr, new double[] { 0.7, 0.3 }, null);
      sf.exec().get();
      Key[] ksplits = sf._destination_frames;
      trainFrame = (Frame)ksplits[0].get();
      testFrame = (Frame)ksplits[1].get();

      XGBoostModel.XGBoostParameters parms = new XGBoostModel.XGBoostParameters();
      parms._ntrees = 5;
      parms._max_depth = 5;
      parms._train = trainFrame._key;
      parms._valid = testFrame._key;
      parms._response_column = response;

      model = new hex.tree.xgboost.XGBoost(parms).trainModel().get();
      Log.info(model);

      preds = model.score(testFrame);
      Assert.assertTrue(model.testJavaScoring(testFrame, preds, 1e-6));
      Assert.assertEquals(
          ((ModelMetricsBinomial)model._output._validation_metrics).auc(),
          ModelMetricsBinomial.make(preds.vec(2), testFrame.vec(response)).auc(),
          1e-5
      );
      Assert.assertTrue(preds.anyVec().sigma() > 0);

    } finally {
      Scope.exit();
      if (trainFrame!=null) trainFrame.remove();
      if (testFrame!=null) testFrame.remove();
      if (tfr!=null) tfr.remove();
      if (preds!=null) preds.remove();
      if (model!=null) model.delete();
    }
  }

  @Test
  public void WeatherBinaryCV() {
    Frame tfr = null;
    Frame trainFrame = null;
    Frame testFrame = null;
    Frame preds = null;
    XGBoostModel model = null;
    try {
      Scope.enter();
      // Parse frame into H2O
      tfr = parse_test_file("./smalldata/junit/weather.csv");
      // define special columns
      String response = "RainTomorrow";
//      String weight = null;
//      String fold = null;
      Scope.track(tfr.replace(tfr.find(response), tfr.vecs()[tfr.find(response)].toCategoricalVec()));
      // remove columns correlated with the response
      tfr.remove("RISK_MM").remove();
      tfr.remove("EvapMM").remove();
      DKV.put(tfr);

      // split into train/test
      SplitFrame sf = new SplitFrame(tfr, new double[] { 0.7, 0.3 }, null);
      sf.exec().get();
      Key[] ksplits = sf._destination_frames;
      trainFrame = (Frame)ksplits[0].get();
      testFrame = (Frame)ksplits[1].get();


      XGBoostModel.XGBoostParameters parms = new XGBoostModel.XGBoostParameters();
      parms._ntrees = 5;
      parms._max_depth = 5;
      parms._train = trainFrame._key;
      parms._valid = testFrame._key;
      parms._nfolds = 5;
      parms._response_column = response;

      model = new hex.tree.xgboost.XGBoost(parms).trainModel().get();
      Log.info(model);

      preds = model.score(testFrame);
      Assert.assertTrue(model.testJavaScoring(testFrame, preds, 1e-6));
      Assert.assertEquals(
          ((ModelMetricsBinomial)model._output._validation_metrics).auc(),
          ModelMetricsBinomial.make(preds.vec(2), testFrame.vec(response)).auc(),
          1e-5
      );
      Assert.assertTrue(preds.anyVec().sigma() > 0);

    } finally {
      Scope.exit();
      if (trainFrame!=null) trainFrame.remove();
      if (testFrame!=null) testFrame.remove();
      if (tfr!=null) tfr.remove();
      if (preds!=null) preds.remove();
      if (model!=null) {
        model.deleteCrossValidationModels();
        model.delete();
      }
    }
  }

  @Test
  public void ProstateRegression() {
    Frame tfr = null;
    Frame trainFrame = null;
    Frame testFrame = null;
    Frame preds = null;
    XGBoostModel model = null;
    Scope.enter();
    try {
      // Parse frame into H2O
      tfr = parse_test_file("./smalldata/prostate/prostate.csv");

      Scope.track(tfr.replace(1, tfr.vecs()[1].toCategoricalVec()));   // Convert CAPSULE to categorical
      Scope.track(tfr.replace(3, tfr.vecs()[3].toCategoricalVec()));   // Convert RACE to categorical
      DKV.put(tfr);

      // split into train/test
      SplitFrame sf = new SplitFrame(tfr, new double[] { 0.7, 0.3 }, null);
      sf.exec().get();
      Key[] ksplits = sf._destination_frames;
      trainFrame = (Frame)ksplits[0].get();
      testFrame = (Frame)ksplits[1].get();

      // define special columns
      String response = "AGE";
//      String weight = null;
//      String fold = null;

      XGBoostModel.XGBoostParameters parms = new XGBoostModel.XGBoostParameters();
      parms._train = trainFrame._key;
      parms._valid = testFrame._key;
      parms._response_column = response;
      parms._ignored_columns = new String[]{"ID"};

      model = new hex.tree.xgboost.XGBoost(parms).trainModel().get();
      Log.info(model);

      preds = model.score(testFrame);
      Assert.assertTrue(model.testJavaScoring(testFrame, preds, 1e-6));
      Assert.assertEquals(
          ((ModelMetricsRegression)model._output._validation_metrics).mae(),
          ModelMetricsRegression.make(preds.anyVec(), testFrame.vec(response), DistributionFamily.gaussian).mae(),
          1e-5
      );
      Assert.assertTrue(preds.anyVec().sigma() > 0);

    } finally {
      Scope.exit();
      if (trainFrame!=null) trainFrame.remove();
      if (testFrame!=null) testFrame.remove();
      if (tfr!=null) tfr.remove();
      if (preds!=null) preds.remove();
      if (model!=null) {
        model.delete();
      }
    }
  }

  @Test
  public void ProstateRegressionCV() {
    for (XGBoostModel.XGBoostParameters.DMatrixType dMatrixType : XGBoostModel.XGBoostParameters.DMatrixType.values()) {
      Frame tfr = null;
      Frame trainFrame = null;
      Frame testFrame = null;
      Frame preds = null;
      XGBoostModel model = null;
      try {
        // Parse frame into H2O
        tfr = parse_test_file("./smalldata/prostate/prostate.csv");

        // split into train/test
        SplitFrame sf = new SplitFrame(tfr, new double[] { 0.7, 0.3 }, null);
        sf.exec().get();
        Key[] ksplits = sf._destination_frames;
        trainFrame = (Frame)ksplits[0].get();
        testFrame = (Frame)ksplits[1].get();

        // define special columns
        String response = "AGE";
//      String weight = null;
//      String fold = null;

        XGBoostModel.XGBoostParameters parms = new XGBoostModel.XGBoostParameters();
        parms._dmatrix_type = dMatrixType;
        parms._nfolds = 5;
        parms._train = trainFrame._key;
        parms._valid = testFrame._key;
        parms._response_column = response;
        parms._ignored_columns = new String[]{"ID"};

        model = new hex.tree.xgboost.XGBoost(parms).trainModel().get();
        Log.info(model);

        preds = model.score(testFrame);
        Assert.assertTrue(model.testJavaScoring(testFrame, preds, 1e-6));
        Assert.assertTrue(preds.anyVec().sigma() > 0);

      } finally {
        if (trainFrame!=null) trainFrame.remove();
        if (testFrame!=null) testFrame.remove();
        if (tfr!=null) tfr.remove();
        if (preds!=null) preds.remove();
        if (model!=null) {
          model.delete();
          model.deleteCrossValidationModels();
        }
      }
    }
  }

  @Test
  public void MNIST() {
    Frame tfr = null;
    Frame preds = null;
    XGBoostModel model = null;
    Scope.enter();
    try {
      // Parse frame into H2O
      tfr = parse_test_file("bigdata/laptop/mnist/train.csv.gz");
      Scope.track(tfr.replace(784, tfr.vecs()[784].toCategoricalVec()));   // Convert response 'C785' to categorical
      DKV.put(tfr);

      // define special columns
      String response = "C785";

      XGBoostModel.XGBoostParameters parms = new XGBoostModel.XGBoostParameters();
      parms._ntrees = 3;
      parms._max_depth = 3;
      parms._train = tfr._key;
      parms._response_column = response;

      model = new hex.tree.xgboost.XGBoost(parms).trainModel().get();
      Log.info(model);

      preds = model.score(tfr);
      Assert.assertTrue(model.testJavaScoring(tfr, preds, 1e-6));
      preds.remove(0).remove();
      Assert.assertTrue(preds.anyVec().sigma() > 0);
      Assert.assertEquals(
          ((ModelMetricsMultinomial)model._output._training_metrics).logloss(),
          ModelMetricsMultinomial.make(preds, tfr.vec(response), tfr.vec(response).domain()).logloss(),
          1e-5
      );
    } finally {
      if (tfr!=null) tfr.remove();
      if (preds!=null) preds.remove();
      if (model!=null) model.delete();
      Scope.exit();
    }
  }

  @Ignore
  @Test
  public void testCSC() {
    Frame tfr = null;
    Frame preds = null;
    XGBoostModel model = null;
    Scope.enter();
    try {
      // Parse frame into H2O
      tfr = parse_test_file("csc.csv");
      String response = "response";

      XGBoostModel.XGBoostParameters parms = new XGBoostModel.XGBoostParameters();
      parms._ntrees = 3;
      parms._max_depth = 3;
      parms._train = tfr._key;
      parms._response_column = response;

      model = new hex.tree.xgboost.XGBoost(parms).trainModel().get();
      Log.info(model);

      preds = model.score(tfr);
      Assert.assertTrue(model.testJavaScoring(tfr, preds, 1e-6));
      Assert.assertTrue(preds.vec(2).sigma() > 0);
      Assert.assertEquals(
          ((ModelMetricsBinomial)model._output._training_metrics).logloss(),
          ModelMetricsBinomial.make(preds.vec(2), tfr.vec(response), tfr.vec(response).domain()).logloss(),
          1e-5
      );
    } finally {
      if (tfr!=null) tfr.remove();
      if (preds!=null) preds.remove();
      if (model!=null) model.delete();
      Scope.exit();
    }
  }
}
