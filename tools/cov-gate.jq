# Branch-coverage gate over `cargo llvm-cov report --json`.
# Pure modules must reach 100 % branches; everything else is only reported. Pure: the value
# layer (domain, plan, config) and the persistent actor's mode machine.

def pure: test("/crates/amoeboid/src/(domain/|plan/|config/|persistent/machine\\.rs$)");

[.data[].files[] | select(.filename | pure)
 | {file: .filename, count: .summary.branches.count, covered: .summary.branches.covered}] as $pure
| ($pure | map(select(.covered < .count))) as $short
| ($pure | map("pure \(.covered)/\(.count) branches \(.file)") | .[]),
  (if ($short | length) == 0
   then "rs-cov: \($pure | length) pure files at 100 % branches"
   else ($short | map("rs-cov: under 100 % branches: \(.file)") | .[]), error("rs-cov: failed")
   end)
