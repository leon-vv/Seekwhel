make build/tests
./build/tests pure
if [ $? -ne 0 ]; then
    utop -init ./utop-seekwhel.ml
fi
./build/tests database

