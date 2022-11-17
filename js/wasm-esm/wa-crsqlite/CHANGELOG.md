# @vlcn.io/wa-crsqlite

## 0.1.10

### Patch Changes

- fixes site id not being passed during replication
- Updated dependencies
  - @vlcn.io/wa-sqlite@0.8.8
  - @vlcn.io/xplat-api@0.1.4

## 0.1.9

### Patch Changes

- cache per connection

## 0.1.8

### Patch Changes

- fix statement preparation error in cases where there are multiple concurrent db connections
- Updated dependencies
  - @vlcn.io/wa-sqlite@0.8.7
  - @vlcn.io/xplat-api@0.1.3

## 0.1.7

### Patch Changes

- update sqlite binaries
- Updated dependencies
  - @vlcn.io/wa-sqlite@0.8.6
  - @vlcn.io/xplat-api@0.1.2

## 0.1.6

### Patch Changes

- use `globalThis` not window

## 0.1.5

### Patch Changes

- tx queue to prevent tx within tx

## 0.1.4

### Patch Changes

- include sources in npm packages

## 0.1.3

### Patch Changes

- debug logging, fatal on bad binds
- Updated dependencies
  - @vlcn.io/wa-sqlite@0.8.5

## 0.1.2

### Patch Changes

- allow callers to specify path to wasm

## 0.1.1

### Patch Changes

- remove `link:../` references so we actually correctly resolve packages
- Updated dependencies
  - @vlcn.io/wa-sqlite@0.8.4
  - @vlcn.io/xplat-api@0.1.1

## 0.1.0

### Minor Changes

- first release that works end to end

### Patch Changes

- Updated dependencies
  - @vlcn.io/xplat-api@0.1.0