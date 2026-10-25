---
type: fix
"TursoSync"
---

The Windows natives (`win-x64`, `win-arm64`) now link the MSVC C runtime statically, so they load on a machine without the Visual C++ redistributable installed. Before, such a machine failed to load `turso_sync_sdk_kit.dll` with "the specified module could not be found", which `TursoNativeLibrary.IsAvailable()` reported as no native library at all.
