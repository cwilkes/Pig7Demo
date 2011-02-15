#!/usr/bin/Rscript
        
args <- commandArgs(TRUE)

png(args[1])
xrange <- c(000,1000)
yrange <- c(000,1000)  
planets  <- read.table("data/in/my-planetsData.tsv", header=TRUE)
clusters <- read.table("output/clusters2.tsv", header=TRUE)
coordboxes <- matrix(c(clusters$WIDTH, clusters$HEIGHT), ncol=2)
symbols(planets$X,planets$Y, planets$BRIGHT, inches=FALSE, xlim=xrange, ylim=yrange)
symbols(clusters$X,clusters$Y, clusters$BRIGHT, inches=FALSE, xlim=xrange, ylim=yrange, bg="yellow", add=TRUE)
symbols(clusters$X,clusters$Y, rectangles=coordboxes, inches=FALSE, xlim=xrange, ylim=yrange, add=TRUE)
dev.off()
