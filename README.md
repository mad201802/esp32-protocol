# ESP32 Protocol

Source Code für meine Bachelorarbeit "Implementierung und Evaluation eines Kommunikationsmodells auf ESP32-basierten Ethernet-Netzwerken innerhalb eines Elektro-Gokarts"

## 📋 Überblick

Das Repository besteht aus zwei Projekten:

- **protocol**: Eine plattformunabhängige Rust-Bibliothek, die das Kommunikationsprotokoll implementiert
  - **Service Discovery (SD)**: Multicast-basierte Diensterkennung
  - **Remote Procedure Calls (RPC)**: Methodenaufrufe
  - **Publish/Subscribe**: Event-basierte Kommunikation
- **embedded-poc**: Ein Proof of Concept für den ESP32, das die Protocol Library nutzt
  - Beispielanwendungen und Evaluationen

## 🛠️ Installation

### Voraussetzungen

#### Allgemeine Anforderungen

- **Rust** (mindestens 1.87): [Installation](https://www.rust-lang.org/tools/install)
- **Git**: Für das Klonen des Repositories
- **Docker** (optional): Für Evaluationen und Tests

#### ESP32-spezifische Anforderungen

Für die Entwicklung mit ESP32-Hardware:

1. **Rust ESP32 Toolchain**:
   ```bash
   # ESP Rust Toolchain installieren
   cargo install ldproxy
   cargo install espup
   espup install
   . ~/export-esp.sh
   ```

2. **Hardware-Verbindung**:
   - ESP32-Entwicklungsboard mit Ethernet (z.B. Olimex ESP32-POE)
   - USB-Kabel für Programmierung und Debugging

### Repository klonen

```bash
git clone https://github.com/mad201802/esp32-protocol.git
cd esp32-protocol
```

### Protocol Library (Standalone)

Die Protocol Library kann unabhängig von ESP32-Hardware verwendet werden:

```bash
cd protocol

# Dependencies installieren
cargo build

# Tests ausführen
cargo test

# Beispiel-Server starten
cargo run --example _dev_server

# In einem anderen Terminal: Beispiel-Client starten
cargo run --example _dev_client
```

### ESP32 Embedded Implementation

Für die ESP32-Implementierung:

```bash
cd embedded-poc

# ESP32-spezifische Dependencies installieren
cargo build

# Auf ESP32 flashen (Hardware muss angeschlossen sein)
cargo run

# Monitoring der seriellen Ausgabe
cargo run -- monitor
```

### Docker-basierte Evaluation

Für Tests und Evaluationen ohne Hardware:

```bash
cd protocol

# Examples bauen
./scripts/build_examples.sh

# Development Environment starten
docker-compose up

# Spezifische Evaluationen ausführen
docker-compose -f eval/docker-compose.fa1.yml up 
docker-compose -f eval/docker-compose.nfa2.yml up
```

## 🔧 Troubleshooting

### Häufige Probleme

1. **ESP32 Compilation Errors**:
   ```bash
   # Toolchain neu installieren
   espup update
   . ~/export-esp.sh
   ```

2. **Netzwerkprobleme**:
   - Multicast-Support des Netzwerk-Switches überprüfen
   - IP-Adressen-Bereich anpassen

3. **Docker Netzwerkprobleme**:
   ```bash
   # Docker Networks zurücksetzen
   docker network prune
   docker-compose down && docker-compose up
   ```

### Debug-Modus

Ausführliches Logging aktivieren:
```bash
RUST_LOG=debug cargo run --example _dev_server
```

---

## KI-Disclaimer

Im Rahmen dieser Bachelorarbeit wurde bei der Entwicklung des Codes das KI-gestützte Tool [GitHub Copilot](https://github.com/features/copilot) verwendet. Vorschläge und Code-Snippets von GitHub Copilot wurden geprüft, angepasst und in eigenen Kontext eingebettet. Die finale Verantwortung für den gesamten Code und dessen Funktionsweise liegt beim Autor dieser Arbeit.

*Dieses Projekt wurde im Rahmen der Abschlussarbeit "Implementierung und Evaluation eines Kommunikationsmodells auf ESP32-basierten Ethernet-Netzwerken innerhalb eines Elektro-Gokarts" entwickelt.*