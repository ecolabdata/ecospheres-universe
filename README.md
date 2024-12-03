# ecospheres-universe

## Mise en place

Utiliser un ficher d'environnement (`env.yaml` dans les exemples ci-dessous) pour stocker la configuration :

```yaml
env: demo
api:
  url: https://demo.data.gouv.fr
  token: ...
```

## Créer ou mettre à jour un univers

```shell
python feed-universe.py [--dry-run] [options] env.yaml
```

Options :
- `--fail-on-error` : Abort on error.
- `--keep-empty` : Conserve les organisations ne contenant pas de datasets pour la génération de la section `organizations`.
- `--reset` : Vide l'univers de tous ses datasets avant de le repeupler. Si la conservation de l'id du topic existant n'a pas d'importance, il est plus simple de supprimer/recréer le topic.
- `--slow` : À combiner avec `--reset` si l'univers contient beaucoup de jeux de données (milliers+) pour éviter les timeouts de l'API data.gouv.


## Mettre à jour le déploiement correspondant

Une fois le script terminé, il faut mettre à jour la section `organizations` du fichier de config [`udata-front-kit`](https://github.com/opendatateam/udata-front-kit/blob/main/configs/ecospheres/config.yaml) correspondant à l'environnement data.gouv mis à jour.

Pour ecologie.data.gouv :
- demo.data.gouv.fr : Mettre à jour la config de la branche `main`, puis merger dans la branche `ecospheres-demo`.
- www.data.gouv.fr : Mettre à jour la config de la branche `ecospheres-prod`.

Puis, dans le cas où la config a changé, faire une release et déploiement de l'environnement mis à jour.
