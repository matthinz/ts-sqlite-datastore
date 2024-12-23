import fs from "node:fs/promises";
import ts from "typescript";

type Chunk = {
  type: "comment";
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
  const sourceFile = ts.createSourceFile(
    file,
    sourceText,
    ts.ScriptTarget.Latest,
  );

  const chunks: Chunk[] = [];
  collectChunks(sourceFile, sourceText, chunks);

  const markdown = buildMarkdown(chunks);

  const packageJSON = JSON.parse(await fs.readFile("package.json", "utf-8"));

  return SUBSTITUTIONS.reduce<string>((result, { find, replaceWith }) => {
    return result.replace(find, replaceWith(packageJSON));
  }, markdown);
}

function buildMarkdown(chunks: Chunk[]): string {
  return chunks
    .map((chunk) => {
      if (chunk.type === "comment") {
        if (chunk.text.startsWith("/**")) {
          // Disregard docblocks
          return;
        } else if (chunk.text.startsWith("//")) {
          // Disregard line comments
          return;
        } else if (chunk.text.startsWith("/*")) {
          return chunk.text.substring(2, chunk.text.length - 2);
        } else {
          return chunk.text;
        }
      } else {
        return "```ts\n" + chunk.text.trim() + "\n```";
      }
    })
    .filter(Boolean)
    .map((text) => text!.trim())
    .join("\n\n");
}

function collectChunks(node: ts.Node, sourceText: string, chunks: Chunk[]) {
  const leadingComments = (
    ts.getLeadingCommentRanges(sourceText, node.getFullStart()) ?? []
  ).map((tsComment) => {
    return {
      type: "comment",
      text: sourceText.substring(tsComment.pos, tsComment.end),
      pos: tsComment.pos,
      end: tsComment.end,
    } as Chunk;
  });

  leadingComments.forEach((c) => {
    if (!anyChunkContains(c)) {
      chunks.push(c);
    }
  });

  const trailingComments = (
    ts.getTrailingCommentRanges(sourceText, node.getEnd()) ?? []
  ).map((tsComment) => {
    return {
      type: "comment",
      text: sourceText.substring(tsComment.pos, tsComment.end),
      pos: tsComment.pos,
      end: tsComment.end,
    } as Chunk;
  });

  trailingComments.forEach((c) => {
    if (!anyChunkContains(c)) {
      chunks.push(c);
    }
  });

  ts.forEachChild(node, (c) => collectChunks(c, sourceText, chunks));

  function anyChunkContains(c: Chunk): boolean {
    return chunks.some((chunk) => {
      return chunk.pos <= c.pos && chunk.end >= c.end;
    });
  }
}
