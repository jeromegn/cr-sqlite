#!/usr/bin/env node

import commandLineArgs from "command-line-args";
import commandLineUsage from "command-line-usage";
import migrate from "./migrate.js";
import chalk from "chalk";

async function run() {
  const mainDefinitions = [{ name: "migrate", defaultOption: true }];

  const mainOptions = commandLineArgs(mainDefinitions, {
    stopAtFirstUnknown: true,
  });
  const argv = mainOptions._unknown || [];

  if (mainOptions.migrate !== "migrate") {
    print_general_usage();
    return;
  }

  const migrateDefitions = [
    { name: "src", alias: "s", defaultOption: true },
    { name: "dest", alias: "d" },
    { name: "tables", alias: "t", multiple: true },
    { name: "overwrite", alias: "w", type: Boolean },
  ];

  const migrateOptions = commandLineArgs(migrateDefitions, { argv });

  if (
    Object.keys(migrateOptions).length === 0 ||
    !migrateOptions.src ||
    !migrateOptions.dest
  ) {
    print_migrate_help();
    return;
  }

  console.log(migrateOptions);
  try {
    migrate(
      migrateOptions.src,
      migrateOptions.dest,
      migrateOptions.tables,
      migrateOptions.overwrite
    );
  } catch (e) {
    if (e.type === "invariant") {
      console.log(chalk.red(e.message));
      return;
    }
    throw e;
  }
}

function print_general_usage() {
  const sections = [
    {
      header: "🧚‍♀️ cfsql 🧚‍♀️",
      content:
        "Utility to migrate existing sqlite databases to be conflict free",
    },
    {
      header: "Synopsis",
      content: "{bold $ cfsql} <command> <options>",
    },
    {
      header: "Commands",
      content: [
        {
          name: "{bold migrate}",
          summary: "Migrate a database 🚀",
        },
      ],
    },
    {
      content:
        "Project home: {underline https://github.com/tantaman/conflict-free-sqlite}",
    },
  ];
  const usage = commandLineUsage(sections);
  console.log(usage);
}

function print_migrate_help() {
  const usage = commandLineUsage([
    {
      header: "cfsql `migrate`",
      content:
        "Migrate a source database, copying it to a dest database which is conflict free",
    },
    {
      header: "Options",
      optionList: [
        {
          name: "src",
          type: String,
          multiple: true,
          defaultOption: true,
          typeLabel: "{underline db_file}",
          description: "database file to process",
          alias: "s",
        },
        {
          name: "dest",
          description: "file to write the migrated copy to",
          type: String,
          typeLabel: "{underline db_file}",
          alias: "d",
        },
        {
          name: "tables",
          description: "only migrate a subset of tables",
          type: String,
          typeLabel: "{underline table} ...",
          alias: "t",
        },
        {
          name: "overwrite",
          description: "allow overwriting the dest file if it exists",
          type: Boolean,
          alias: "w",
        },
      ],
    },
  ]);

  console.log(usage);
}

run().catch((e) => console.error(e));