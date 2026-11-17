import { cpSync } from "node:fs";
cpSync(new URL("../static/", import.meta.url), new URL("../dist/", import.meta.url), {
  recursive: true,
});
