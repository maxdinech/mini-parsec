# Mini-Parsec

![Logo de Parsec](https://github.com/Scille/parsec-cloud/blob/master/docs/parsec_doc_logo.png)

## Utilisation

Il faut un serveur PostgreSQL avec une base `mini-parsec` existante.

La création de tables est gérée automatiquement par le script.

### Datasets

Le téléchargement de deux datasets (Enron et Gutenberg) est automatisé par :

```Python
python -m mini-parsec datasets
```

### Serveur

```Python
python -m mini-parsec server --key [KEYWORD]
```

Le flag `--reset` permet de vider les BDD et d'effacer les données client et serveur.

Quand le script affiche `Watching folder data/client/`, on peut lancer des copies de fichiers dans le dossier `data/client`. Par exemple :

```Python
cp -r data/Enron/* data/client/
```

### Recherche

```Python
python -m mini-parsec search --key [KEYWORD] --query [WORD]
```

### Repack

```Python
python -m mini-parsec repack --key [KEYWORD]
```

### Re-chiffrement

```Python
python -m mini-parsec repack --key [KEYWORD] --newkey [NEW KEYWORD]
```

