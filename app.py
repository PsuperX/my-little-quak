import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
from flask import abort, render_template, Flask
import logging
import db

APP = Flask(__name__)


# Start page
@APP.route("/")
def index():
    stats = {}
    stats = db.execute(
        """
    SELECT * FROM
      (SELECT COUNT() n_occur FROM Ocorrencias)
    JOIN
      (SELECT COUNT() n_armas FROM Armas)
    JOIN
      (SELECT COUNT() n_areas FROM Areas)
    JOIN
      (SELECT COUNT() n_locais FROM Locais)
    JOIN
      (SELECT COUNT() n_vitimas FROM Vitimas)
    JOIN
      (SELECT COUNT() n_crimes FROM Crimes)
    """
    ).fetchone()
    logging.info(stats)
    return render_template("index.html", stats=stats)


# Ocorrencias
@APP.route("/ocorrencias/")
def list_ocorrencias():
    ocorrencias = db.execute(
        """
      SELECT occId, date_occ, date_rptd, hora
      FROM Ocorrencias
      ORDER BY date_occ desc
      """
    ).fetchall()
    return render_template("ocorrencias-list.html", ocorrencias=ocorrencias)


@APP.route("/ocorrencias/<int:id>/")
def get_movie(id):
    ocorrencias = db.execute(
        """
      SELECT date_occ, date_rptd, hora
      FROM Ocorrencias
      WHERE occId = ?
      """,
        [id],
    ).fetchone()

    if ocorrencias is None:
        abort(404, "Movie id {} does not exist.".format(id))

    crimes = db.execute(
        """
      SELECT crimeId, crimes.desc_crime
      FROM ocorrencias NATURAL JOIN occ_crime NATURAL JOIN crimes
      WHERE occId = ?
      ORDER BY crimeId
      """,
        [id],
    ).fetchall()

    vitimas = db.execute(
        """
      SELECT vitimaId, idade, sexo, descendencia, status
      FROM correncias NATURAL JOIN vitimas
      WHERE occId = ?
      ORDER BY vitimaId
      """,
        [id],
    ).fetchall()

    areas = db.execute(
        """
      SELECT armaId, descricao
      FROM Ocorrencias NATURAL JOIN occ_armas NATURAL JOIN Armas
      WHERE occId = ?
      ORDER BY armaId
      """,
        [id],
    ).fetchall()

    locais = db.execute(
        """
      SELECT localId descricao, nome as area, coordenadas, morada,
      FROM Ocorrencias NATURAL JOIN Locais NATURAL JOIN Areas
      WHERE occId = ?
      ORDER BY localId
      """,
        [id],
    ).fetchall()

    return render_template(
        "ocorrencia.html", ocorrencias=ocorrencias, crimes=crimes, vitimas=vitimas, areas=areas, locais=locais
    )



# Locais
@APP.route("/locais/")
def list_locais():
    locais = db.execute(
        """
      SELECT localId, coordenadas, morada, descricao
      FROM Locais
      ORDER BY descricao
    """
    ).fetchall()
    return render_template("local-list.html", locais=locais)


@APP.route("/locais/<int:id>/")
def view_movies_by_actor(id):
    locais = db.execute(
        """
    SELECT localId, coordenadas, morada, descricao
    FROM Locais
    WHERE localId = ?
    """,
        [id],
    ).fetchone()

    if locais is None:
        abort(404, "Local id {} does not exist.".format(id))

    ocorrencias = db.execute(
        """
    SELECT localId descricao, nome as area, coordenadas, morada,
    FROM Ocorrencias NATURAL JOIN Locais NATURAL JOIN Areas
    WHERE occId = ?
    ORDER BY localId
    """,
        [id],
    ).fetchall()

    return render_template("local.html", locais=locais, ocorrencias=ocorrencias)


@APP.route("/locais/search/<expr>/")
def search_local(expr):
    search = {"expr": expr}
    locais = db.execute(
        """ SELECT localId, coordenadas, morada, descricao
        FROM Locais
        WHERE descricao = ?
        """
    ).fetchall()

    return render_template("local-search.html", search=search, locais=locais)


#Crimes
@APP.route("/crimes/")
def list_crimes():
    crimes = db.execute(
        """
      SELECT crimeId, desc_crime as descrição
      FROM Crimes
      ORDER BY descrição
    """
    ).fetchall()
    return render_template("crime-list.html", crimes=crimes)


@APP.route("/crimes/<int:id>/")
def view_ocorriencias_by_crime(id):
    crime = db.execute(
        """
    SELECT crimeId, desc_crime as descrição
    FROM Crimes
    WHERE crimeId = ?
    """,
        [id],
    ).fetchone()

    if crime is None:
        abort(404, "Genre id {} does not exist.".format(id))

    ocorrencias = db.execute(
        """
    SELECT date_occ, date_rptd, hora
    FROM Ocorrencias NATURAL JOIN occ_crime NATURAL JOIN Crimes
    WHERE crimeId = ?
    ORDER BY date_occ
    """,
        [id],
    ).fetchall()

    return render_template("crime.html", crime=crime, ocorrencias=ocorrencias)

@APP.route("/crimes/search/<expr>/")
def search_crime(expr):
    search = {"expr": expr}
    crime = db.execute(
        """ SELECT crimeId, desc_crime
        FROM Crimes
        WHERE desc_crime = ?
        """
    ).fetchall()

    return render_template("crime-search.html", search=search, crime=crime)

# Vitimas
@APP.route("/vitimas/")
def list_vitimas():
    vitimas = db.execute(
        """
      SELECT vitimaId, idade, sexo, descendencia, status
      FROM Vitimas
      order by vitimaId
    """
    ).fetchall()
    return render_template("vitimas-list.html", vitimas=vitimas)


@APP.route("/vitimas/<int:id>/")
def get_vitima(id):
    vitima = db.execute(
        """
      SELECT vitimaId, idade, sexo, descendencia, status
      FROM Vitimas
      WHERE vitimaId = ?
      """,
        [id],
    ).fetchone()

    if vitima is None:
        abort(404, "Stream id {} does not exist.".format(id))

    return render_template("vitima.html", vitima=vitima)


# Armas
@APP.route("/areas/")
def list_armas():
    areas = db.execute(
        """
      SELECT armaId, descricao
      FROM Armas
      ORDER BY armaId
    """
    ).fetchall()
    return render_template("areas-list.html", areas=areas)


@APP.route("/areas/<int:id>/")
def view_ocorriencias_by_arma(id):
    arma = db.execute(
        """
    SELECT armaId, descricao
    FROM  Armas
    WHERE armaId = ?
    ORDER BY armaId
    """,
        [id],
    ).fetchone()

    if arma is None:
        abort(404, "Arma id {} does not exist.".format(id))

    ocorrencias = db.execute(
        """
    SELECT date_occ, date_rptd, hora
    FROM Ocorrencias NATURAL JOIN occ_armas NATURAL JOIN Armas
    WHERE armaId = ?
    ORDER BY date_occ
    """,
        [id],
    ).fetchall()

    return render_template("arma.html", arma=arma, ocorrencias=ocorrencias)

@APP.route("/arma/search/<expr>/")
def search_arma(expr):
    search = {"expr": expr}
    arma = db.execute(
        """ SELECT crimeId, desc_crime as descrição
        FROM Crimes
        WHERE descrição = ?
        """
    ).fetchall()

    return render_template("arma-search.html", search=search, arma=arma)

# Areas
@APP.route("/areas/")
def list_areas():
    areas = db.execute(
        """
      SELECT areaId, nome
      FROM Areas
      ORDER BY nome
    """
    ).fetchall()
    return render_template("areas-list.html", areas=areas)


@APP.route("/areas/<int:id>/")
def view_ocorriencias_by_arma(id):
    area = db.execute(
        """
    SELECT areaId, nome
    FROM Areas
    WHERE areaId = ?
    ORDER BY nome
    """,
        [id],
    ).fetchone()

    if arma is None:
        abort(404, "Area id {} does not exist.".format(id))

    ocorrencias = db.execute(
        """
    SELECT date_occ, date_rptd, hora
    FROM Ocorrencias NATURAL JOIN Areas
    WHERE areaId = ?
    ORDER BY date_occ
    """,
        [id],
    ).fetchall()

    return render_template("area.html", area=area, ocorrencias=ocorrencias)

@APP.route("/area/search/<expr>/")
def search_area(expr):
    search = {"expr": expr}
    area = db.execute(
        """ SELECT areaId, nome
        FROM Areas
        WHERE nome = ?
        ORDER BY nome
        """
    ).fetchall()

    return render_template("area-search.html", search=search, area=area)
