#!/bin/bash

utop ./maintenance/tests.ml pure


#if [ $? -ne 0 ]; then
#    utop -init ./utop-seekwhel.ml
#fi

utop ./maintenance/tests.ml database

