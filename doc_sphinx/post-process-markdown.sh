#!/bin/bash


function edit_file() {
    # replace inter-doc links "[class_or_func](link_in_doc)" to "`class_or_func`"
    sed -i -E "s/\[([^]\*]+)\]\([^\)]+\)/\`\1\`/g" $1

    # replace links to 3rd party "[*something*](url)" to "*something*"
    sed -i -E "s/\[([^]]+)\]\([^\)]+\)/\1/g" $1

    # replace func / method / class definition header "##### <class/func/meth>" to "`<class/func/meth>`"
    sed -i -E "s/(\#{2,}) ([^\(]+)\((.*)\)$/\1 \`\`\2(\3)\`\`/g" $1

    # replace \* to * if line starts with # character to make *args and **kwargs more clear
    sed -i -E "/^#/s/[\]?\*/\*/g" $1

    # replace "*class*" to "class" in headers
    sed -i -E "/^#/s/(\#{2,}) \`\`\*([^\*]+)\*/\1 \`\`\2/" $1

    # adjust property since it doesn't match pervious patterns
    sed -i -E "/^#/s/(\#{2,}) \*property\* (.*)/\1 \`\`property \2\`\`/" $1

    # replace ipython3 with python code block
    sed -i -E "s/\`\`\`ipython3/\`\`\`python/g" $1

    # replace "### Examples" to "##### Examples" to attach it to any previous header while splitting
    sed -i -E "s/^### Examples/##### Examples/g" $1

    # replace "#### SEE ALSO" to "##### SEE ALSO" to attach it to any previous header while splitting
    sed -i -E "s/^#### SEE ALSO/##### SEE ALSO/g" $1

    # replace redundant notes in code block like "```pycon" or "```default"; leave it simply as a code block opening
    # keep only python code block
    sed -i -E "s/\`\`\`((python)?)(.*)/\`\`\`\1/g" $1

    # remove unnecessary "<br/>"
    sed -i -E "s/<br\/>//g" $1

    # remove reference html tags
    sed -i -E "s/^<a id.*a>$//g" $1

    # remove <div> html tags
    sed -i -E "s/^<div.*>$//g" $1
    sed -i -E "s/^<\/div>//g" $1

    # remove fixed 21 lines footer
    head -n -21 $1 > tmp.f && mv tmp.f $1

    # remove deprecation warnings that my occur
    sed -i -E "s/.*DeprecationWarning.*//g" $1
}

for f in $(find . -name '*.md' | grep _build/markdown); do
    echo $f
    edit_file $f
done
