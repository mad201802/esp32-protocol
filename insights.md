# Insights (während der Entwicklung)

1. Die komplette Netzerkkommunikation (SD & RPC) kann nicht über einen UDPSocket realisiert werden, da diese über separate Ports kommunizieren sollen, was den Netzwerkverkehr übersichtlicher macht.
2. Die Service Discovery über den Broadcast umzusetzen hat nicht funktioniert, da jeder Client seine eigenen Broadcast Pakete wieder empfängt und somit der Buffer zu schnell voll wird. Lösung: Multicast verwenden.