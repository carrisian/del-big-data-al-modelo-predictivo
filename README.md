# Del Big Data al modelo predictivo

**Monitorización ambiental inteligente en la Región de Murcia**

## Descripción del Proyecto
Este repositorio alberga el ecosistema tecnológico creado para la Situación de Aprendizaje **"Del Big Data al modelo predictivo: Implementación de un ecosistema en Google Colab para la monitorización ambiental inteligente en la Región de Murcia"**.

El proyecto trasciende el análisis convencional de estaciones físicas mediante el uso de datos satelitales del programa **Copernicus** (CAMS, ERA5 y EAC4). El objetivo es dotar al aula de Tecnología de 2º de Bachillerato de herramientas avanzadas de **Data Science** e **Inteligencia Artificial**, transformando a los alumnos de usuarios pasivos en desarrolladores activos de soluciones técnicas frente al cambio climático.

---

## 📂 Acceso a los Datasets (Paso Obligatorio)

Debido a que el dataset completo supera los **90 GB**, los archivos no se encuentran alojados directamente en GitHub. Para poder ejecutar los cuadernos en Google Colab, es estrictamente necesario seguir estos pasos:

1. **Acceso a la fuente:** Entra en la carpeta pública de Google Drive: [Datasets Proyecto Murcia](https://drive.google.com/drive/folders/1oB8DM8E9twygJRogl2AL0O-AWXpwG9M_?usp=drive_link).
2. **Vincular a tu Drive:** Haz clic en el nombre de la carpeta en la parte superior (`Datasets Proyecto Murcia`) y selecciona la opción **"Añadir acceso directo a Drive"**.
3. **Ubicación:** Selecciona **"Mi unidad"** como destino.
   > **Nota importante:** Este proceso no copia los archivos a tu cuenta ni consume tu espacio de almacenamiento (los 15GB gratuitos). Solo crea un "puente" para que Google Colab pueda leer los archivos de 30GB y 60GB directamente desde la nube.

---

## Ejes Fundamentales
* **Ingeniería de Datos:** Pipelines en Python para la extracción, limpieza y normalización de datasets masivos (más de un millón de registros horarios).
* **Inteligencia Artificial Aplicada:** Implementación de modelos predictivos mediante redes neuronales **LSTM** y algoritmos de Machine Learning para sistemas de alerta temprana.
* **Propuesta Didáctica (ABP):** Diseño de una Situación de Aprendizaje alineada con la LOMLOE, que integra competencias STEM, alfabetización algorítmica y los ODS de la Agenda 2030.

## Componentes Técnicos
- `/src`: *Scripts* de Python para la interactuación con las APIs científicas de Copernicus.
- `/notebooks`: Entornos de **Google Colab** configurados con el "Laboratorio de Datos" para el modelado predictivo.
- `/assets`: Recursos visuales y diagramas de flujo del proceso de Aprendizaje Basado en Proyectos (ABP).

## Tecnologías y Herramientas
* **Lenguajes:** Python.
* **Infraestructura:** Google Colab (Cloud Computing).
* **Fuentes Globales:** Copernicus (CAMS, ERA5-Land, EAC4).
* **Librerías:** `pandas` (gestión de datos), `scikit-learn` (Machine Learning), `tensorflow`/`keras` (redes neuronales).

## Contribución Académica
Este desarrollo forma parte del Trabajo Fin de Máster (TFM) del *Máster Universitario en Formación del Profesorado de Educación Secundaria Obligatoria y Bachillerato, Formación Profesional, Enseñanzas de Idiomas y Enseñanzas Artísticas* (Especialidad en Tecnología).
