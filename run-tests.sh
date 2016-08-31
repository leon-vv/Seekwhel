make build/tests
./build/tests
if [ $? -ne 0 ]; then
    utop -init ./utop-seekwhel.ml
fi

