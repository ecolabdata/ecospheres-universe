# ecospheres-universe

## Mise en place

### Configuration

La configuration est stockée dans un fichier `config-{env}.yaml` en fonction du `env` data.gouv.fr sur lequel on travaille (`demo` ou `prod`).

Pour renseigner les tokens d'API sans fuiter de secrets, utiliser les variables d'environnement `DATAGOUV_API_KEY` et `GRIST_API_KEY` ou surcharger la config à l'aide d'un autre fichier qui ne sera pas commité dans git :

```yaml
datagouv:
  token: ...
grist:
  token: ...
```

### Installation locale

```shell
uv sync
```

### Automatisation via GitHub Actions

Le [workflow `update-universes`](https://github.com/ecolabdata/ecospheres-universe/blob/main/.github/workflows/update-universes.yml) met quotidiennement à jour les univers `demo` et `prod`.


## Utilisation

### Créer ou mettre à jour un univers

Crée ou met à jour un univers à partir de sa définition Grist.

```shell
uv run ecospheres-universe feed-universe [options] config-{env}.yaml [config-overrides.yaml ...]
```

Options :
- `--dry-run` : Perform a trial run without actual feeding
- `--fail-on-errors` : Fail the run on http errors
- `--keep-empty` : Keep empty organizations in the list
- `--reset` : Empty topic before refeeding it
- `--verbose` : Enable verbose mode

En plus de la mise à jour de l'univers sur data.gouv.fr, le script génère plusieurs fichiers `dist/organizations-{type}-{env}.json` contenant les organisations data.gouv.fr qui référencent des objets appartenant à l'univers mis à jour. Ces fichiers peuvent être [utilisés comme API par `udata-front-kit`](https://github.com/opendatateam/udata-front-kit/blob/main/configs/ecospheres/config.yaml).

### Vérifier la synchronisation

Vérifie la synchronisation entre l'index Elasticsearch et la base de données MongoDB de data.gouv.fr pour les organisations de l'univers.

```shell
uv run ecospheres-universe check-sync config-{env}.yaml [config-overrides.yaml ...]
```
