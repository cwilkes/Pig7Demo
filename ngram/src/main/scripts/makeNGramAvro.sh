#!/bin/bash

java -cp lib\* org.seattlehadoop.ngram.input.AvroMaker -input $1 -output $2 -maxsize 1000
