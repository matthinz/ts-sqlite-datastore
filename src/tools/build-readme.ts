import fs from "node:fs/promises";
import ts from "typescript";

type Comment = {
  text: string;
  pos: number;
  end: number;
};

const SUBSTITUTIONS = [
  {
    find: /Version: \d+\.\d+\.\d+/g,
    replaceWith: ({ version }: any) => `Version: ${version}`,
  },
];

run(process.argv.slice(2)).catch((err) => {
  console.error(err);
  process.exit(1);
});

async function run(args: string[]) {
  if (args.length === 0) {
    throw new Error("Must specify at least one file on the command line");
  }

  const promises = args.map(processFile);

  await Promise.all(promises).then((outputs) => {
    console.log(outputs.join("\n"));
  });
}

async function processFile(file: string): Promise<string> {
  const sourceText = ts.sys.readFile(file) || "";
  const comments: Comment[] = [];
  const sourceFile = ts.createSourceFile(
    file,
    sourceText,
    ts.ScriptTarget.Latest,
  );

  collectComments(sourceFile, sourceText, comments);

  const markdown = comments
    .filter((c) => !c.text.startsWith("/**") && c.text.startsWith("/*"))
    .map((c) => c.text.substring(2, c.text.length - 2).trim())
    .join("\n\n");

  const packageJSON = JSON.parse(await fs.readFile("package.json", "utf-8"));

  return SUBSTITUTIONS.reduce<string>((result, { find, replaceWith }) => {
    return result.replace(find, replaceWith(packageJSON));
  }, markdown);
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
