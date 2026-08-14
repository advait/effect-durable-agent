const SPAN_NAME_PATTERN = /^[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)+$/;

const stringValue = (node) =>
  node?.type === "Literal" && typeof node.value === "string" ? node.value : undefined;

/** Validate configured catalogs as static, canonical, and duplicate-free. */
export const spanCatalogFormat = {
  meta: {
    type: "problem",
    docs: {
      description: "enforce canonical lowercase dot-separated span catalog values",
    },
    messages: {
      duplicate: "Span name '{{name}}' is duplicated in the catalog.",
      format:
        "Span name '{{name}}' must contain only lowercase alphanumeric dot-separated segments.",
      static: "Span catalogs must be static arrays of string literals.",
    },
    schema: [
      {
        type: "object",
        properties: {
          catalogs: {
            type: "array",
            items: { type: "string", minLength: 1 },
            minItems: 1,
            uniqueItems: true,
          },
        },
        additionalProperties: false,
        required: ["catalogs"],
      },
    ],
  },
  create(context) {
    const catalogNames = new Set(context.options[0].catalogs);
    const seen = new Set();

    const inspectArray = (array) => {
      for (const element of array.elements) {
        const value = stringValue(element);
        if (value === undefined) {
          context.report({ messageId: "static", node: element ?? array });
          continue;
        }
        if (!SPAN_NAME_PATTERN.test(value)) {
          context.report({ messageId: "format", data: { name: value }, node: element });
        }
        if (seen.has(value)) {
          context.report({ messageId: "duplicate", data: { name: value }, node: element });
        }
        seen.add(value);
      }
    };

    return {
      VariableDeclarator(node) {
        if (node.id.type !== "Identifier" || !catalogNames.has(node.id.name)) {
          return;
        }
        const initializer = node.init;
        const catalog =
          initializer?.type === "CallExpression" && initializer.arguments.length === 1
            ? initializer.arguments[0]
            : initializer;
        if (catalog?.type !== "ArrayExpression") {
          context.report({ messageId: "static", node: initializer ?? node });
          return;
        }
        inspectArray(catalog);
      },
    };
  },
};
