#!/bin/bash
nice -n 19 java -cp target/facebook-stack-1.0-SNAPSHOT-jar-with-dependencies.jar "$@"
