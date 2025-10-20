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
python -m ecospheres_universe feed-universe [options] config-{env}.yaml [env.yaml]
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
python -m ecospheres_universe check-sync config-{env}.yaml [env.yaml]
```


## Utilisation du référentiel

Le script génère un fichier `dist/organizations-{env}.json` avec les informations extraites de data.gouv.fr des organisations valides et possédant des jeux de données. Ce fichier peut être utilisé comme API.
