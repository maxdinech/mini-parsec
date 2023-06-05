# Mini-Parsec

![Logo de Parsec](https://github.com/Scille/parsec-cloud/blob/master/docs/parsec_doc_logo.png)

## Installation

Il faut une installation postgresql.

Après clonage du repo :

```bash
poetry install
sudo -u postgres psql -c 'CREATE DATABASE "mini-parsec";'
```

**Note :** Si besoin, remplacer `python -m` par `poetry run python -m` dans les commandes qui suivent.

## Utilisation

Avant toute chose :

```bash
sudo systemctl start postgresql
```

### Datasets

Le téléchargement de deux datasets (Enron et Gutenberg) est automatisé par :

```Python
python -m mini-parsec datasets
```

### Serveur

Pour lancer le logiciel serveur, chiffré avec le mot-clé `KEYWORD` :

```Python
python -m mini-parsec server --key [KEYWORD]
```

Le flag `--reset` permet de vider les BDD et d'effacer les données client et serveur.

Quand le script affiche `Watching folder data/client/`, on peut lancer des copies de fichiers dans le dossier `data/client`. Par exemple :

```Python
cp -r data/Enron/* data/client/
```

### Recherche

Pour chercher le mot `WORD` dans un serveur chiffré avec le mot-clé `KEYWORD` :

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

