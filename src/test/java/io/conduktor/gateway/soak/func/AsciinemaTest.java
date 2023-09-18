package io.conduktor.gateway.soak.func;

import com.vladsch.flexmark.ast.FencedCodeBlock;
import com.vladsch.flexmark.parser.Parser;
import com.vladsch.flexmark.util.ast.Document;
import com.vladsch.flexmark.util.ast.NodeVisitor;
import com.vladsch.flexmark.util.ast.VisitHandler;
import com.vladsch.flexmark.util.sequence.BasedSequence;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public class AsciinemaTest {

    public static final String PROXY_DEMOS_FOLDER = "/Users/framiere/conduktor/conduktor-proxy-demos";
    String code = "";

    @Test
    public void walk() throws Exception {
        Files.walk(Paths.get(PROXY_DEMOS_FOLDER))
                .filter(p -> "Readme.md".equalsIgnoreCase(p.getFileName().toString()))
                .forEach(path -> extractCode(path.toFile()));
    }


    @SneakyThrows
    public void extractCode(File readme) {
        String markdown = getMarkdown(readme);
        code = "";
        Parser parser = Parser.builder().build();
        Document document = parser.parse(markdown);

        NodeVisitor visitor = new NodeVisitor(
                new VisitHandler<>(FencedCodeBlock.class, this::visit)
        );
        visitor.visit(document);

        File sh = new File(readme.getParent() + "/run.sh");
        sh.setExecutable(true);
        if (StringUtils.isNotBlank(code)) {
            String header = """
                    
                    function execute() {
                        chars=$(echo "$*" | wc -c)
                        sleep 2
                        printf "$"
                        if [ "$chars" -lt 100 ] ; then
                            echo "$*" | pv -qL 50
                        elif [ "$chars" -lt 250 ] ; then
                            echo "$*" | pv -qL 100
                        elif [ "$chars" -lt 500 ] ; then
                            echo "$*" | pv -qL 200
                        else
                            echo "$*" | pv -qL 400
                        fi
                        eval "$*"
                    }

                    """;
            System.out.println(readme.getAbsolutePath() + " converted");
            FileUtils.writeStringToFile(sh, header + code, Charset.defaultCharset());
        } else {
            System.out.println(readme.getAbsolutePath() + " does not have code block");
        }
    }

    private String getMarkdown(File file) {
        try {
            return FileUtils.readFileToString(file, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void visit(FencedCodeBlock codeBlock) {
        BasedSequence info = codeBlock.getInfo();
        if ("bash".equals(info.toString())) {
            String bash = codeBlock.getContentChars().normalizeEOL();
            bash = StringUtils.replace(bash, "\\\n", "\\\\\n");
            bash = StringUtils.replace(bash, "\"", "\\\"");
            code += "execute \"\"\"" + bash + "\"\"\"\n\n";
        }
    }
}
