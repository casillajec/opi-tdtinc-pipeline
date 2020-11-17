# opi-tdtinc-pipeline

Implementation of a data ingestion and preprocessing pipeline for the fictional
company Tortas de Tamal Inc.


## Airflow

Decidi usar Airflow como orquestador de datos dado que ya tengo expriencia
usandolo y no tengo acceso a una cuenta en Azure.


## Datalake

Decidi mantener el datalake como simples carpetas en el file system del
contenedor por simplicidad.

Se espera que la seccion cruda siga la siguiente estructura:

    datalake/
    ├── crudo
    │   └── generador
    │       └── fuente
    │           └── <fecha>
    │               ├── erp
    │               │   ├── ventas_mensuales_<region>.csv
    │               │   ├── ...
    │               └── teinvento
    │                   ├── fact_table
    │                   │   ├──...
    │                   ├── product_dim
    │                   │   ├──...
    │                   └── region_dim
    │                       ├──...

Y la seccion preprocesada seguira la siguiente estructura:

    datalake/
    └── preprocesado
        └── generador
            └── fuente
                └── <fecha>
                    ├── erp
                    │   ├── <region>
                    │   │   ├── ...
                    │   ├── ...
                    └── teinvento
                        ├── <empresa>
                        │   ├── <region>
                        │   │   ├── ...
                        │   ├── ...
                        ├── ...


## Como correr

En la raiz del repositorio se incluye un `Dockerfile` que define la imagen
para correr el pipeline, se puede construir con:

    docker build . -t <nombreimagen>

Luego se puede correr un contenedor con dicha imagen:

    docker run --rm --network=host --name=tdtcont <nombreimagen>

Esta imagen corre el servidor y un scheduler secuencial de `Airflow`, tambien
tiene la informacion proveida en la seccion cruda del datalake. Al iniciarlo
tambien se iniciara el pipeline para el `2020-08-01` de manera automatica.

Se pueden explorar los resultados con:

    docker exec tdtcont ls datalake/procesado/generador/fuente/20200801/

Una vez el pipeline haya corrido se programara la corrida para el `2020-08-08`
(siguiente semana) y asi sucesivamente, si se quiere probar la automatizacion
se puede incluir en la seccion cruda del datalake la informacion obtenida
para el `2020-08-08`. Es importante que los datos se copien en secuencia
cronologica pues el ejecutor que se configuro en `Airflow` es secuencial,
lo cual implica que no se pueden paralelizar tareas, y por lo tanto los
pipelines se correran uno por uno en secuencia cronologica de manera
automatica.
