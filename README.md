# Mini-Parsec

## Utilisation

Il faut un serveur PostgreSQL avec une base `mini-parsec` existante.

La création de tables est gérée automatiquement par le script.

Pour lancer le script :

```Python
python -m mini-parsec sync --delete
```

`--delete` sert à réinitialiser les dossiers `data/server`, `data/client` et les BDD.

Quand le script affiche `Watching folder ...`, on peut lancer des copies de fichiers dans le dossier `data/client`. Par exemple :

```Python
cp -r data/Enron/* data/client/
```

La recherche s'effectue avec la commande :

```Python
python -m mini-parsec search --query [WORD]
```
