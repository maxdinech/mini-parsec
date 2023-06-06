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
python -m miniparsec datasets
```

Deux datasets sont fournis :

- **Enron :**
- **Gutenberg :**

### Serveur

Pour lancer le logiciel serveur, chiffré avec le mot-clé `KEYWORD` :

```Python
python -m miniparsec server --key [KEYWORD]
```

Le flag `--reset` permet de vider les BDD et d'effacer les données client et serveur.

Quand le script affiche `Watching folder data/client/`, on peut lancer des copies de fichiers dans le dossier `data/client`. Par exemple :

```Python
cp -r data/Enron/* data/client/
```

### Recherche

Pour chercher le mot `WORD` dans un serveur chiffré avec le mot-clé `KEYWORD` :

```Python
python -m miniparsec search --key [KEYWORD] --query [WORD]
```

Le flag `--show` permet d'afficher la liste des résultats.

Pour une recherche multiple (par défaut en intersection) :

**Intersection :**
```Python
python -m miniparsec search --key [KEYWORD] --query [WORD1+WORD2+WORD3]
```

**Union :**

```Python
python -m miniparsec search --key [KEYWORD] --query [WORD1+WORD2+WORD3] --union
```

### Repack

```Python
python -m miniparsec repack --key [KEYWORD]
```

### Re-chiffrement

```Python
python -m miniparsec repack --key [KEYWORD] --newkey [NEW KEYWORD]
```
