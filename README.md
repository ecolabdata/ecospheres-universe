# ecospheres-universe

## Mise en place

Installer les dépendances avec [uv](https://docs.astral.sh/uv/):

```shell
uv sync
```

La configuration est stockée dans un fichier `config-{env}.yaml` en fonction du `env` sur lequel on travaille (`demo` ou `prod`).

Pour renseigner le token d'API, utiliser la variable d'environnement `DATAGOUV_API_KEY` ou un autre fichier d'environnement :

```yaml
api:
  url: https://demo.data.gouv.fr
  token: ...
```

## Créer ou mettre à jour un univers

```shell
uv run ecospheres-universe feed-universe [options] config-{env}.yaml [config-overrides.yaml ...]
```

Options :
- `--dry-run` : Perform a trial run without actual feeding
- `--fail-on-errors` : Fail the run on http errors
- `--keep-empty` : Keep empty organizations in the list
- `--reset` : Empty topic before refeeding it
- `--verbose` : Enable verbose mode

## Vérifier la synchronisation

Vérifie la synchronisation entre l'index Elasticsearch de data.gouv.fr et la base de données MongoDB de data.gouv.fr pour les organisations de l'univers.

```shell
uv run ecospheres-universe check-sync config-{env}.yaml [config-overrides.yaml ...]
```


## Utilisation du référentiel

Le script génère un fichier `dist/organizations-{env}.json` avec les informations extraites de data.gouv.fr des organisations valides et possédant des jeux de données. Ce fichier peut être utilisé comme API.
