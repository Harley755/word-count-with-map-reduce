#!/bin/bash
# ==============================================================================
# MAP REDUCE CLASSIQUE - SCRIPT D'EXÉCUTION
# ==============================================================================
# Configuration
DATASET="data/corpus_1_5gb.txt"
OUTPUT="results_classic"

echo "🚀 MAP REDUCE CLASSIQUE - WORD COUNT"
echo "📂 Dataset: $DATASET"
echo "💾 Output: $OUTPUT"
echo "====================================="

# Installation des dépendances si nécessaire
if ! python3 -c "import pyspark" 2>/dev/null; then
    echo "📦 Installation PySpark..."
    pip install -r requirements.txt
fi

# Création répertoire sortie
mkdir -p "$OUTPUT"

# Exécution
python mapreduce_classic.py

echo "✅ Terminé!"
