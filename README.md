# Word Count Distribué - MapReduce Classique

## Projet

Implémentation **MapReduce pure** d'un Word Count distribué avec Apache Spark pour traiter votre dataset de 1.5GB.

## Structure du Projet

```
big_data/
├── download_corpus.py      # Script pour télécharger le corpus
├── mapreduce_classic.py    # Code MapReduce classique
├── run_classic.sh          # Script d'exécution
├── requirements.txt        # Dépendances PySpark
├── data/                   # Dataset (généré par download_corpus.py)
└── wordcount_complet/      # Résultats de sortie
```

## Utilisation

### 1. Installation des dépendances

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou .venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### 2. Génération du corpus (OBLIGATOIRE)

Le corpus de 1.5GB n'est pas inclus dans le projet. Il faut le générer :

```bash
python download_corpus.py
```

**Ce script va :**
- Télécharger 74 livres classiques depuis Project Gutenberg
- Créer le fichier `data/corpus.txt` (~75MB)
- Pour obtenir le corpus complet de 1.5GB, modifiez la liste `BOOK_IDS` dans le script

**Pour générer corpus_1_5gb.txt :**
1. Ajoutez plus d'IDs de livres dans la liste `BOOK_IDS`
2. Relancez `python download_corpus.py`
3. Renommez `data/corpus.txt` en `data/corpus_1_5gb.txt`

### 3. Exécution du Word Count

```bash
./run_classic.sh
```

### Manuel
```bash
python mapreduce_classic.py
```

## Architecture MapReduce

### Phase 1: Chargement Distribué
- Fichier découpé en **49 partitions**
- Chaque partition traitée par un cœur CPU différent
- Le fichier n'est jamais chargé entièrement en mémoire

### Phase 2: MAP
- Chaque ligne de texte est découpée en mots avec regex
- Mots convertis en minuscules (`Think` → `think`)
- Création des tickets `(mot, 1)`

### Phase 3: SHUFFLE + REDUCE
- Regroupement automatique des tickets par mot
- Addition des compteurs : `(think, [1,1,1,1])` → `(think, 4)`

### Phase 4: TRI et Sauvegarde
- Tri par fréquence décroissante
- Sauvegarde dans un **seul fichier CSV**

## Résultats

### Performance sur votre dataset
- **Tickets générés**: 220,796,224
- **Mots uniques**: 111,609
- **Top 10 mots**:
  1. the → 15,107,202 occurrences
  2. and → 9,699,734 occurrences
  3. that → 3,797,068 occurrences
  4. was → 3,010,964 occurrences
  5. his → 2,804,274 occurrences

### Fichier de sortie
- **Chemin**: `wordcount_complet/`
- **Format**: CSV avec colonnes `word,count`
- **Contenu**: Tous les 111,609 mots triés par fréquence
- **Structure**: Un seul fichier grâce à `coalesce(1)`

## 🔧 Configuration

- **Dataset**: `data/corpus_1_5gb.txt` (à générer avec download_corpus.py)
- **Partitions**: 49 (automatique)
- **Optimisation**: `reduceByKey` (shuffle minimal)
- **Sérialisation**: Kryo serializer

## Notes importantes

**Dataset non inclus** : Le corpus de 1.5GB n'est pas fourni avec le projet pour des raisons de taille. Vous devez le générer vous-même avec `download_corpus.py`.

**Source des données** : Project Gutenberg (libre de droits)
- 74 livres inclus par défaut (~75MB)
- Ajoutez plus d'IDs pour atteindre 1.5GB
- IDs disponibles sur : https://www.gutenberg.org/

**Temps de génération** :
- corpus.txt (74 livres) : ~5-10 minutes
- corpus_1_5gb.txt (200-300 livres) : ~30-60 minutes