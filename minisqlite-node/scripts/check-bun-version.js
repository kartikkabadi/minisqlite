if (!Bun.semver.satisfies(Bun.version, ">=1.4.4")) {
  console.error("Bun 1.4.4 or newer is required to run the binding tests.");
  process.exit(1);
}
