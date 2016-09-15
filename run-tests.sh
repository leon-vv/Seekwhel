utop ./build/tests.ml pure

if [ $? -ne 0 ]; then
    utop -init ./utop-seekwhel.ml
fi

utop ./build/tests database

