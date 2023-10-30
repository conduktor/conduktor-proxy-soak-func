for stepSh in $(ls step*sh | sort ) ; do
    echo "Processing $stepSh " `date`
    step=$(echo "$stepSh" | sed "s/.sh$//" )
    sh -x $stepSh > output/$step.txt


    if ! awk 'NF{exit 1}' output/$step.txt; then
      echo """
<details>
<summary>Command output</summary>

\`\`\`sh
""" > output/$step.md

      cat output/$step.txt >> output/$step.md

      echo """
\`\`\`

</details>
      """ >> output/$step.md

      awk '
        BEGIN { content = ""; tag = "'$step-OUTPUT'" }
        FNR == NR { content = content $0 ORS; next }
        { gsub(tag, content); print }
      ' output/$step.md Readme.md > temp.txt && mv temp.txt Readme.md

    else

      gsed -i "s/$step-OUTPUT//gi" Readme.md

    fi

done
