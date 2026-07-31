const EFFECT_MODULES = new Set(["effect", "effect/Effect"]);
const SPAN_APIS = new Set([
  "fn",
  "makeSpan",
  "makeSpanScoped",
  "useSpan",
  "withSpan",
  "withSpanScoped",
]);

const isIdentifier = (node) => node?.type === "Identifier";

const isAnonymousEffectFn = (node) =>
  node?.type === "FunctionExpression" || node?.type === "ArrowFunctionExpression";

const resolvedImportIs = (sourceCode, identifier, importSpecifiers) => {
  let scope = sourceCode.getScope(identifier);
  while (scope !== null) {
    const variable = scope.set.get(identifier.name);
    if (variable !== undefined) {
      return variable.defs.some(
        (definition) =>
          definition.type === "ImportBinding" && importSpecifiers.has(definition.node),
      );
    }
    scope = scope.upper;
  }
  return false;
};

const importedName = (specifier) =>
  specifier.imported.type === "Identifier" ? specifier.imported.name : specifier.imported.value;

const isStringLiteral = (node) => node?.type === "Literal" && typeof node.value === "string";

const spanNameArgument = (api, args) => {
  if (api === "fn") {
    return isAnonymousEffectFn(args[0]) ? undefined : args[0];
  }
  if (api !== "withSpan" && api !== "withSpanScoped") {
    return args[0];
  }
  if (args[1] !== undefined && !isStringLiteral(args[0]) && args[1].type !== "ObjectExpression") {
    return args[1];
  }
  return args[0];
};

/** Require named Effect spans to use a static name registered in the configured catalog. */
export const effectSpanFromCatalog = {
  meta: {
    type: "problem",
    docs: {
      description: "require named Effect spans to use a static name from an allowed catalog",
    },
    messages: {
      dynamic: "Named Effect spans must use a static string literal.",
      unknown: 'Span name "{{name}}" is not registered in the configured catalog.',
    },
    schema: [
      {
        type: "object",
        properties: {
          catalogs: {
            type: "array",
            items: {
              type: "array",
              items: { type: "string", minLength: 1 },
              minItems: 1,
              uniqueItems: true,
            },
            minItems: 1,
          },
        },
        additionalProperties: false,
        required: ["catalogs"],
      },
    ],
  },
  create(context) {
    const sourceCode = context.sourceCode;
    const options = context.options[0];
    const allowedNames = new Set(options.catalogs.flat());
    const effectNamespaceImports = new Set();
    const namedEffectImports = new Map();

    const resolveSpanApi = (callee) => {
      if (isIdentifier(callee)) {
        const imported = namedEffectImports.get(callee.name);
        if (
          imported !== undefined &&
          resolvedImportIs(sourceCode, callee, new Set([imported.specifier]))
        ) {
          return imported.api;
        }
        return undefined;
      }
      if (
        callee?.type !== "MemberExpression" ||
        callee.computed ||
        !isIdentifier(callee.object) ||
        !isIdentifier(callee.property) ||
        !SPAN_APIS.has(callee.property.name) ||
        !resolvedImportIs(sourceCode, callee.object, effectNamespaceImports)
      ) {
        return undefined;
      }
      return callee.property.name;
    };

    return {
      ImportDeclaration(node) {
        const source = node.source.value;
        for (const specifier of node.specifiers) {
          if (EFFECT_MODULES.has(source) && specifier.type === "ImportNamespaceSpecifier") {
            effectNamespaceImports.add(specifier);
          }
          if (
            source === "effect" &&
            specifier.type === "ImportSpecifier" &&
            importedName(specifier) === "Effect"
          ) {
            effectNamespaceImports.add(specifier);
          }
          if (EFFECT_MODULES.has(source) && specifier.type === "ImportSpecifier") {
            const api = importedName(specifier);
            if (SPAN_APIS.has(api)) {
              namedEffectImports.set(specifier.local.name, { api, specifier });
            }
          }
        }
      },
      CallExpression(node) {
        const api = resolveSpanApi(node.callee);
        if (api === undefined) {
          return;
        }
        const name = spanNameArgument(api, node.arguments);
        if (name === undefined) {
          return;
        }
        if (!isStringLiteral(name)) {
          context.report({ messageId: "dynamic", node: name });
          return;
        }
        if (!allowedNames.has(name.value)) {
          context.report({ data: { name: name.value }, messageId: "unknown", node: name });
        }
      },
    };
  },
};
