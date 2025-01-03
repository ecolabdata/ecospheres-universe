# ecospheres-universe

## Mise en place

La configuration est stockée dans un fichier `config-{env}.yaml` en fonction du `env` sur lequel on travaille (`demo` ou `prod`).

Pour renseigner le token d'API, utiliser la variable d'environnement `DATAGOUV_API_KEY` ou un autre fichier d'environnement (`env.yaml` dans les exemples ci-dessous) :

```yaml
api:
  url: https://demo.data.gouv.fr
  token: ...
```

## Créer ou mettre à jour un univers

```shell
python feed-universe.py [--dry-run] [options] config-{env}.yaml env.yaml
```

Options :
- `--fail-on-error` : Abort on error.
- `--keep-empty` : Conserve les organisations ne contenant pas de datasets pour la génération de la section `organizations`.
- `--reset` : Vide l'univers de tous ses datasets avant de le repeupler. Si la conservation de l'id du topic existant n'a pas d'importance, il est plus simple de supprimer/recréer le topic.
- `--slow` : À combiner avec `--reset` si l'univers contient beaucoup de jeux de données (milliers+) pour éviter les timeouts de l'API data.gouv.


## Utilisation du référentiel

Le script génère un fichier `dist/organizations-{env}.json` avec les informations extraites de data.gouv.fr des organisations valides et possédant des jeux de données. Ce fichier peut être utilisé comme API.
