#!/usr/bin/env python3
"""
MAP REDUCE CLASSIQUE - WORD COUNT DISTRIBUTUÉ

Auteur: Expert Big Data - Apache Spark
Date: 2025
Objectif: Implémentation pédagogique du paradigme MapReduce pour Word Count

Dataset: corpus_1_5gb.txt (1.5GB de texte littéraire)
Architecture: Spark RDD avec MapReduce classique
Output: Fichier CSV unique avec tous les mots et leurs occurrences
"""

import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

def main():
    """

    FONCTION PRINCIPALE - MAP REDUCE WORD COUNT

    
    Flux d'exécution:
    1. Initialisation Spark
    2. Chargement distribué du dataset
    3. Phase MAP: Transformation lignes → (mot, 1)
    4. Phase SHUFFLE: Regroupement automatique
    5. Phase REDUCE: Addition des compteurs
    6. Phase TRI: Organisation par fréquence
    7. Sauvegarde complète des résultats
    """
    
    #
    # ÉTAPE 1: INITIALISATION SPARK
    #
    print("ÉTAPE 1: INITIALISATION APACHE SPARK")
    print("=" * 80)
    
    # Création de la session Spark avec optimisations
    spark = SparkSession.builder \
        .appName("MapReduce-WordCount-Classique") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Configuration du niveau de logs (pour affichage clair)
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    
    print("Session Spark initialisée avec succès")
    print(f"Version Spark: {spark.version}")
    print(f"Mode: {sc.master}")
    
    #
    # ÉTAPE 2: CHARGEMENT DISTRIBUÉ DU DATASET
    #
    print("\nÉTAPE 2: CHARGEMENT DISTRIBUÉ DU DATASET")
    print("-" * 80)
    
    # Chemin vers le dataset 1.5GB (chemin relatif)
    input_path = "data/corpus_1_5gb.txt"
    print(f"Fichier source: {input_path}")
    print(f"Taille estimée: 1.5GB")
    
    # Chargement distribué - Spark découpe automatiquement le fichier en partitions
    # Chaque partition sera traitée par un cœur CPU différent
    lines = sc.textFile(input_path)
    num_partitions = lines.getNumPartitions()
    
    print(f"Nombre de partitions créées: {num_partitions}")
    print("Chaque partition est traitée en parallèle par un cœur CPU différent")
    print("Le fichier n'est JAMAIS chargé entièrement en mémoire")
    
    # Comptage des lignes pour validation
    total_lines = lines.count()
    print(f"Nombre total de lignes: {total_lines:,}")
    
    #
    # ÉTAPE 3: PHASE MAP - TRANSFORMATION LIGNES → TICKETS (mot, 1)
    #
    print("\n ÉTAPE 3: PHASE MAP - TRANSFORMATION")
    print("-" * 80)
    
    def map_function(line):
        """
        FONCTION MAP - CŒUR DE LA TRANSFORMATION
        
        Input:  Une ligne de texte brute du dataset
        Output: Liste de paires (mot, 1) appelées "tickets"
        """
        # Étape 1: Conversion en minuscules et extraction des mots
        # Regex \b[a-z]+\b capture uniquement les mots de lettres
        words = re.findall(r'\b[a-z]+\b', line.lower())
        
        # Étape 2: Création des tickets (mot, 1)
        # On ignore les mots de moins de 3 caractères pour réduire le bruit
        tickets = []
        for word in words:
            if len(word) > 2:  # Filtrage des mots trop courts
                tickets.append((word, 1))
        
        return tickets
    
    # Application de la phase MAP sur toutes les partitions
    # flatMap étend chaque ligne en 0, 1 ou plusieurs tickets
    print("Application de la transformation MAP sur toutes les partitions...")
    tickets = lines.flatMap(map_function)
    
    # Comptage et affichage des statistiques
    total_tickets = tickets.count()
    print(f"Tickets générés: {total_tickets:,}")
    print(f"Moyenne: {total_tickets / total_lines:.1f} tickets par ligne")
    
    
    #
    # ÉTAPE 5: PHASE REDUCE - ADDITION DES COMPTEURS
    #
    print("\nÉTAPE 5: PHASE REDUCE - ADDITION DES COMPTEURS")
    print("-" * 80)

    
    # Application de la phase REDUCE avec reduceByKey (OPTIMAL)
    print("Application de reduceByKey (optimisation réseau)...")
    word_counts = tickets.reduceByKey(lambda a, b: a + b)
    
    # Statistiques des résultats
    unique_words = word_counts.count()
    print(f"Mots uniques trouvés: {unique_words:,}")
    print(f"Ratio unicité: {unique_words / total_tickets * 100:.2f}%")
    
    #
    # ÉTAPE 6: SHUFFLE ET PHASE TRI - ORGANISATION PAR FRÉQUENCE
    #
    print("\nÉTAPE 6: SHUFFLE ET PHASE TRI - ORGANISATION PAR FRÉQUENCE")
    print("-" * 80)
    print("Tri des mots par fréquence décroissante")
    print("Processus: inversion (count, word) → tri → réinversion")
    
    # Étape 1: Inversion pour tri par compteur (count devient la clé)
    # Exemple: ("the", 1000) → (1000, "the")
    inverted = word_counts.map(lambda x: (x[1], x[0]))
    
    # Étape 2: Tri par compteur décroissant
    sorted_inverted = inverted.sortByKey(ascending=False)
    
    # Étape 3: Réinversion pour format final (word, count)
    sorted_words = sorted_inverted.map(lambda x: (x[1], x[0]))
    
    # Affichage des 10 premiers mots (aperçu pour le rapport)
    print("\nAPERÇU - TOP 10 MOTS LES PLUS FRÉQUENTS:")
    print("-" * 50)
    top_preview = sorted_words.take(10)
    
    for i, (word, count) in enumerate(top_preview, 1):
        # Formatage pour affichage clair dans le rapport
        percentage = (count / total_tickets) * 100
        print(f"{i:2d}. {word:<15} → {count:>10,} occurrences ({percentage:.2f}%)")
    
    #
    # ÉTAPE 7: SAUVEGARDE COMPLÈTE DES RÉSULTATS
    #
    print("\nÉTAPE 7: SAUVEGARDE COMPLÈTE DES RÉSULTATS")
    print("-" * 80)
    print(f"Sauvegarde de TOUS les {unique_words:,} mots uniques")
    print("Format: CSV avec colonnes (word, count)")
    print("Tri: par fréquence décroissante")
    print("Structure: UN SEUL FICHIER grâce à coalesce(1)")
    
    # Configuration du chemin de sortie (chemin relatif)
    output_path = "wordcount_complet"
    
    # Conversion en DataFrame pour sauvegarde CSV structurée
    print("Conversion en DataFrame Spark...")
    results_df = spark.createDataFrame(word_counts, ["word", "count"])
    
    # Sauvegarde avec optimisations:
    # - orderBy(desc("count")): tri par fréquence décroissante
    # - coalesce(1): fusion en un seul fichier (pour rapport)
    # - mode("overwrite"): écrasement si existe déjà
    print("Sauvegarde en cours...")
    results_df.orderBy(desc("count")) \
              .coalesce(1) \
              .write.mode("overwrite") \
              .option("header", "true") \
              .csv(output_path)
    
    print(f"Sauvegarde terminée: {output_path}")
    print("Fichier unique créé avec tous les résultats")
    
    #
    # RÉSUMÉ FINAL POUR LE RAPPORT
    #
    print("\n" + "=" * 80)
    print("RÉSUMÉ DE L'EXÉCUTION MAP REDUCE")
    print(f"Dataset: {input_path}")
    print(f"Taille: 1.5GB")
    print(f"Partitions: {num_partitions}")
    print(f"Lignes traitées: {total_lines:,}")
    print(f"Tickets générés: {total_tickets:,}")
    print(f"Mots uniques: {unique_words:,}")
    print(f"Fichier sortie: {output_path}")
    print(f"Mot le plus fréquent: {top_preview[0][0]} ({top_preview[0][1]:,} fois)")
    
    #
    # NETTOYAGE DES RESSOURCES
    #
    print("\nNETTOYAGE DES RESSOURCES SPARK")
    spark.stop()
    print("Session Spark arrêtée proprement")
    print("WORD COUNT MAP REDUCE TERMINÉ AVEC SUCCÈS!")

if __name__ == "__main__":
    """

    POINT D'ENTRÉE PRINCIPAL

    
    Gestion des erreurs et exécution du programme principal.
    """
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERREUR LORS DE L'EXÉCUTION: {e}")
        print("Vérifiez:")
        print("  - PySpark est bien installé")
        print("  - Le fichier dataset existe et est accessible")
        print("  - Les permissions d'écriture sont correctes")
        sys.exit(1)
