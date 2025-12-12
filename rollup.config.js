import commonjs from "@rollup/plugin-commonjs";
import resolve from "@rollup/plugin-node-resolve";
import typescript from "@rollup/plugin-typescript";
import terser from "@rollup/plugin-terser";
import json from "@rollup/plugin-json";

export default {
    input: "main.ts",
    output: {
        file: "dist/main.js",
        format: "es",
        banner: "#!/usr/bin/env node",
    },
    plugins: [resolve(), commonjs(), json(), typescript(), terser()],
};
