import js from '@eslint/js';
import globals from 'globals';

export default [
  js.configs.recommended,
  {
    files: ['**/*.js'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: { ...globals.node, ...globals.browser },
    },
    plugins: {
      import: await import('eslint-plugin-import')
    },
    rules: {
      // “Airbnb-ish” essentials for Node
      'no-var': 'error',
      'prefer-const': 'error',
      'eqeqeq': ['error', 'smart'],
      'no-unused-vars': ['warn', { args: 'none', ignoreRestSiblings: true }],

      // import plugin bits commonly enforced by Airbnb
      'import/no-unresolved': 'error',
      'import/named': 'error',
      'import/order': ['warn', { 'newlines-between': 'always' }],
      'import/no-extraneous-dependencies': ['error', { devDependencies: true }],

      'max-len': ['error', { code: 150, ignoreUrls: true, ignoreStrings: true, ignoreTemplateLiterals: true, ignoreComments: true }],
      'quotes': ['error', 'single', { avoidEscape: true, allowTemplateLiterals: true }],

      'indent': ['error', 2, { 'SwitchCase': 1 }],
      'comma-dangle': ['error', 'always-multiline'],
      // allow await in for...of but not other loops
      'no-await-in-loop': 'off',
      'no-restricted-syntax': [
        'error',
        { selector: 'ForStatement:has(AwaitExpression)', message: 'Avoid await in plain for-loops; prefer for…of or batching.' },
        { selector: 'WhileStatement:has(AwaitExpression)', message: 'Avoid await in while-loops; consider restructuring.' },
        { selector: 'DoWhileStatement:has(AwaitExpression)', message: 'Avoid await in do…while-loops; consider restructuring.' },
        { selector: 'ForInStatement:has(AwaitExpression)', message: 'Avoid await in for…in; iterate keys and batch instead.' }
      ]
    }
  },
];
