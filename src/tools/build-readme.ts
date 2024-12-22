import ts from "typescript";

type Comment = {
  text: string;
  pos: number;
  end: number;
};

run(process.argv.slice(2)).catch((err) => {
  console.error(err);
  process.exit(1);
});

async function run(args: string[]) {
  const promises = args.map(async (file) => {
    const sourceText = ts.sys.readFile(file) || "";
    const comments: Comment[] = [];
    const sourceFile = ts.createSourceFile(
      file,
      sourceText,
      ts.ScriptTarget.Latest,
    );

    collectComments(sourceFile, sourceText, comments);

    comments
      .filter((c) => !c.text.startsWith("/**") && c.text.startsWith("/*"))
      .map((c) => c.text.substring(2, c.text.length - 2).trim())
      .forEach((c) => console.log(`${c}\n`));
  });

  await Promise.all(promises);
}

function collectComments(
  node: ts.Node,
  sourceText: string,
  comments: Comment[],
) {
  addComments(ts.getLeadingCommentRanges(sourceText, node.getFullStart()));
  addComments(ts.getTrailingCommentRanges(sourceText, node.getEnd()));

  node.forEachChild((child) => {
    collectComments(child, sourceText, comments);
  });

  function addComments(ranges: ts.CommentRange[] | undefined) {
    if (!ranges) return;

    ranges.forEach((tsComment) => {
      const isDuplicate = comments.some((c) => {
        return c.pos === tsComment.pos && c.end === tsComment.end;
      });

      if (isDuplicate) {
        return;
      }

      comments.push({
        text: sourceText.substring(tsComment.pos, tsComment.end),
        pos: tsComment.pos,
        end: tsComment.end,
      });
    });
  }
}
