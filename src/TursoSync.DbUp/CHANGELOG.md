# TursoSync.DbUp

## 1.7.0

### Patch Changes

- Updated dependencies
  - TursoSync@1.7.0

## 1.6.0

### Patch Changes

- Updated dependencies
  - TursoSync@1.6.0

## 1.5.0

### Patch Changes

- Updated dependencies
  - TursoSync@1.5.0

## 1.4.5

### Patch Changes

- Updated dependencies
  - TursoSync@1.4.5

## 1.4.4

### Patch Changes

- Updated dependencies
  - TursoSync@1.4.4

## 1.4.3

### Patch Changes

- Updated dependencies
  - TursoSync@1.4.3

## 1.4.2

### Patch Changes

- Updated dependencies
  - TursoSync@1.4.2

## 1.4.1

### Patch Changes

- Updated dependencies
  - TursoSync@1.4.1

## 1.4.0
### Patch Changes

- Updated dependencies
  - TursoSync@1.4.0

## 1.3.0
### Patch Changes

- Updated dependencies
  - TursoSync@1.3.0

## 1.2.0
### Patch Changes

- Updated dependencies
  - TursoSync@1.2.0

## 1.1.0
### Patch Changes

- Updated dependencies
  - TursoSync@1.1.0

## 1.0.1
### Patch Changes

- Fix DbUp journal existence check on Turso: compare sqlite_master table names with COLLATE NOCASE. The Turso engine case-folds identifier names in sqlite_master (real SQLite preserves case), so the case-sensitive lookup missed the SchemaVersions journal on every connection after the one that created it — DbUp then re-ran CREATE TABLE and failed with "table already exists", breaking all migration runs after the first.
- Updated dependencies
  - TursoSync@1.0.1

## 1.0.0
### Patch Changes

- Updated dependencies
  - TursoSync@1.0.0

## 0.1.0
### Patch Changes

- Updated dependencies
  - TursoSync@0.1.0
