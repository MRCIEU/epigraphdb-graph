library(data.table)
library(foreach)
library(doParallel)
library(psych)
library(corrplot)
library(caret)
#library(Hmisc)
library(WGCNA);

phesantDir='/path/to/sep_phesant_files/'
outDir=phesantDir

impute <- function(){
  print ('imputing')
  files <- list.files(path=phesantDir, pattern="*.txt$", full.names=T, recursive=FALSE)
  lapply(files, function(x) {
    print(x)
    data = fread(x,sep=' ',header=T)
    #print(head(data))
    data_imputed = setDF(data)
    num_cols <- dim(data)[2]
    #print(num_cols)
    colCount=0
    for(i in 2:ncol(data_imputed)){
       if( i %% 1000 == 0 ) print(paste(i,Sys.time()))
       data_imputed[is.na(data_imputed[,i]), i] <- mean(data_imputed[,i], na.rm = TRUE)
     }

    fwrite(data_imputed,paste0(outDir,basename(x),'.imputed'),verbose=TRUE,nThread = 4,sep = ' ')
  })
}

cor2pvalue = function(r, n) {
  t <- (r*sqrt(n-2))/sqrt(1-r^2)
  p <- 2*(1 - pt(abs(t),(n-2)))
  se <- sqrt((1-r*r)/(n-2))
  out <- list(r, n, t, p, se)
  names(out) <- c("r", "n", "t", "p", "se")
  return(out)
}

#takes 24 hours using 10 cores
correlate <- function(){
  #setup parallel backend to use many processors
  cores=detectCores()
  cl <- makeCluster(2) #not to overload your computer
  registerDoParallel(cl)

  print('correlating')
  files <- list.files(path=outDir, pattern="*.txt.imputed.gz$", full.names=T, recursive=FALSE)
  for (i in 1:length(files)){
    f = files[i]
    print(files[i])
    df1=fread(paste('gunzip -c ',files[i]),sep=' ',header=T)
    #if (i+1<=length(files)){
      foreach (j=i:length(files), .combine = rbind, .packages=c('data.table','psych'), .export=c("outDir","cor2pvalue")) %dopar% {
      #for (j in j:length(files)){
        print(paste(i,j))
        df2=fread(paste('gunzip -c ',files[j]),sep=' ',header=T)
        #c=cor(df1[,3:ncol(df1)],df2[,3:ncol(df2)],method="spearman")
		#does not work with ci=FALSE
        c=corr.test(df1[,3:ncol(df1)],df2[,3:ncol(df2)],method='spearman',ci=TRUE, adjust='none')
        #c=rcorr(df1[,3:ncol(df1)],df2[,3:ncol(df2)],type='spearman')
		x=df1[,3:ncol(df1)]
		y=df2[,3:ncol(df2)]
		n <- t(!is.na(x)) %*% (!is.na(y)) # same as count.pairwise(x,y) from psych package
		r <- cor(x, y, method="spearman") # MUCH MUCH faster than corr.test()
		#c=cor2pvalue(r,n)
        #print(c)
        print(Sys.time())
	    write.csv(c$r,file=gzfile(paste0(outDir,basename(files[i]),'_',basename(files[j]),'.corr.test.cor.gz')))
	    write.csv(c$p,file=gzfile(paste0(outDir,basename(files[i]),'_',basename(files[j]),'.corr.test.pval.gz')))
      }
    #}
  }
}

impute()
correlate()
