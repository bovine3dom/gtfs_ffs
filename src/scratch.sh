#!/bin/fish

# get all headers
for f in **.txt;
    echo -n (basename $f) ": "; head -n1 $f;
end | sort | uniq
