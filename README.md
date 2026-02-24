# ecospheres-universe

## Mise en place

### Configuration Grist

Le script `ecospheres-universe` doit avoir accès à la définition de l'univers dans Grist.

Pour cela, ajouter aux utilisateurs du document le compte de service 4edf618b-6d1e-4914-b4e7-d6ec14e10289@serviceaccounts.invalid en "lecture seule".

### Configuration du script

La configuration est stockée dans un fichier `configs/{site}-{env}.yaml` en fonction de la verticale et de l'environnement data.gouv.fr (`demo` ou `prod`) ciblés.

Pour renseigner les tokens d'API sans fuiter de secrets, utiliser les variables d'environnement `DATAGOUV_API_KEY` et `GRIST_API_KEY` ou surcharger la configuration à l'aide d'un autre fichier qui ne sera pas commité dans git :

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

Le [workflow `update-universes`](https://github.com/ecolabdata/ecospheres-universe/blob/main/.github/workflows/update-universes.yml) met quotidiennement à jour les univers configurés.

Pour ajouter un univers au workflow :
1. [Créer un environnement github](https://github.com/ecolabdata/ecospheres-universe/settings/environments) en suivant la convention `{site}-{env}`.
2. Configurer le secret `DATAGOUV_API_KEY` pour le nouvel environnement. Si Grist n'utilise pas le compte de service par défaut, configurer également le secret `GRIST_API_KEY`.
3. Ajouter l'environnement à `jobs.run-update.strategy.matrix.universe` dans le [workflow]((https://github.com/ecolabdata/ecospheres-universe/blob/main/.github/workflows/update-universes.yml)).


## Utilisation

### Créer ou mettre à jour un univers

Crée ou met à jour un univers à partir de sa définition Grist.

```shell
uv run ecospheres-universe feed-universe [options] configs/{site}-{env}.yaml [config-overrides.yaml ...]
```

Options :
- `--dry-run` : Perform a trial run without actual feeding
- `--fail-on-errors` : Fail the run on http errors
- `--reset` : Empty topic before refeeding it
- `--verbose` : Enable verbose mode

En plus de la mise à jour de l'univers sur data.gouv.fr, le script génère plusieurs fichiers `dist/{site}-{env}/organizations-{type}.json` contenant les organisations data.gouv.fr qui référencent des objets appartenant à l'univers mis à jour. Ces fichiers peuvent être [utilisés comme API par `udata-front-kit`](https://github.com/opendatateam/udata-front-kit/blob/main/configs/ecospheres/config.yaml).

### Vérifier la synchronisation

Vérifie la synchronisation entre l'index Elasticsearch et la base de données MongoDB de data.gouv.fr pour les organisations de l'univers.

```shell
uv run ecospheres-universe check-sync configs/{site}-{env}.yaml [config-overrides.yaml ...]
```
