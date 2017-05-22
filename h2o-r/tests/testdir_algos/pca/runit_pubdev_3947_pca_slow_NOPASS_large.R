setwd(normalizePath(dirname(R.utils::commandArgs(asValues=TRUE)$"f")))
source("../../../scripts/h2o-r-test-setup.R")

# Test PCA on car.arff.txt
test.pca.slow <- function() {

  data = h2o.uploadFile(locate("bigdata/laptop/jira/re0.wc.arff.txt.zip"),destination_frame = "data",header = T)
  data = data[,-2887]

  ptm <- proc.time()
  mm = h2o.prcomp(data,transform = "STANDARDIZE",k =1504, pca_method="GLRM", , use_all_factor_levels=TRUE)
  h2otimepassed = proc.time() - ptm
  print(h2otimepassed)
  h2o.rm(mm)
  h2o.rm(data)


  ptm <- proc.time()
  mm = h2o.prcomp(data,transform = "STANDARDIZE",k =1504, max_iterations=10, pca_method="Randomized")
  h2otimepassed = proc.time() - ptm
  print(h2otimepassed)
  h2o.rm(mm)


  ptm <- proc.time()
  mm = h2o.prcomp(data,transform = "STANDARDIZE",k =1504, pca_method="GramSVD")
  h2otimepassed = proc.time() - ptm
  print(h2otimepassed)
  h2o.rm(mm)

  ptm <- proc.time()
  mm = h2o.prcomp(data,transform = "STANDARDIZE",k =1504, pca_method="Power")
  h2otimepassed = proc.time() - ptm
  print(h2otimepassed)
  h2o.rm(mm)
  
  data = read.csv(locate("bigdata/laptop/jira/re0.wc.arff.txt.zip"))
  data = data[,-2887]
  ptm <- proc.time()
  fitR <- prcomp(data, center = T, scale. = T)
  timepassed = proc.time() - ptm
  print(timepassed)


}

doTest("PCA Test: rec0.wc.arff.txt", test.pca.slow)
