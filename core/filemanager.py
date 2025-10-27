import json
import logging
from os import makedirs
from os.path import dirname, realpath
from pathlib import Path
import pdftotext
import fitz
from pytesseract import image_to_string
from PIL import Image
import re

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


class FileManager:
    """
    Da funcionalidad de lectura (load) y escritura (dump) de ficheros
    """
    OCR_SUFFIX = ".ocr.txt"

    def __init__(self, root=None):
        """
        Parameters
        ----------
        root: str | Path
            por defecto es la raiz del proyecto, es decir, el directorio ../.. al de este fichero
            se usa para interpretar que las rutas relativas son relativas a este directorio root
        """
        if root is None:
            root = Path(dirname(realpath(__file__))).parent
        elif isinstance(root, str):
            root = Path(root)

        self.root = root

    def resolve_path(self, file) -> Path:
        """
        Si es una ruta absoluta se devuelve tal cual
        Si es una ruta relativa se devuelve bajo la ruta root
        Si empieza por ~ se expande

        Parameters
        ----------
        file: str | Path
            Ruta a resolver
        """
        if isinstance(file, str):
            file = Path(file)

        if str(file).startswith("~"):
            file = file.expanduser()

        if file.is_absolute():
            return file

        root_file = self.root.joinpath(file)

        return root_file

    def normalize_ext(self, ext: str) -> str:
        """
        Normaliza extensiones para identificar el tipo de fichero en base a la extension
        """
        ext = ext.lstrip(".")
        ext = ext.lower()
        return {
            "xlsx": "xls",
            "js": "json",
            "sql": "txt",
            "csv": "txt",
            "htm": "html"
        }.get(ext, ext)

    def load(self, file, *args, not_exist_ok=False, **kargv):
        """
        Lee un fichero en funcion de su extension
        Para que haya soporte para esa extension ha de exisitir una funcion load_extension
        """
        file = self.resolve_path(file)
        if not_exist_ok and not file.exists():
            return None

        ext = self.normalize_ext(file.suffix)

        load_fl = getattr(self, "load_"+ext, None)
        if load_fl is None:
            raise Exception(f"No existe metodo para leer ficheros {ext} [{file.name}]")

        return load_fl(file, *args, **kargv)

    def dump(self, file, obj, *args, **kargv):
        """
        Guarda un fichero en funcion de su extension
        Para que haya soporte para esa extension ha de exisitir una funcion dump_extension
        """
        file = self.resolve_path(file)
        self.makedirs(file)

        if isinstance(obj, bytes):
            with open(file, "wb") as f:
                f.write(obj)
            return

        if isinstance(obj, str):
            with open(file, "w") as f:
                f.write(obj)
            return

        ext = self.normalize_ext(file.suffix)

        dump_fl = getattr(self, "dump_"+ext, None)
        if dump_fl is None:
            raise Exception(f"No existe metodo para guardar ficheros {ext} [{file.name}]")

        dump_fl(file, obj, *args, **kargv)

    def makedirs(self, file):
        file = self.resolve_path(file)
        makedirs(file.parent, exist_ok=True)

    def load_json(self, file, *args, **kargv):
        with open(file, "r") as f:
            return json.load(f, *args, **kargv)

    def dump_json(self, file, obj, *args, indent=2, **kargv):
        with open(file, "w") as f:
            json.dump(obj, f, *args, indent=indent, **kargv)

    def load_html(self, file, *args, parser="lxml", **kargv):
        with open(file, "r") as f:
            return BeautifulSoup(f.read(), parser)

    def dump_html(self, file, obj, *args, **kargv):
        if isinstance(obj, (BeautifulSoup, Tag)):
            obj = str(obj)
        with open(file, "w") as f:
            f.write(obj)

    def load_txt(self, file, *args, **kargv):
        with open(file, "r") as f:
            txt = f.read()
            if args or kargv:
                txt = txt.format(*args, **kargv)
            return txt

    def dump_txt(self, file, txt, *args, **kargv):
        if args or kargv:
            txt = txt.format(*args, **kargv)
        with open(file, "w") as f:
            f.write(txt)

    def load_pdf(self, file, *args, **kwargs):
        with open(file, 'rb') as fl:
            pdf = list(pdftotext.PDF(fl, **kwargs))
            all_text = "\n".join(pdf).rstrip()
            plain_text = "".join(c for c in all_text if c.isprintable()).strip()
            plain_text = re.sub(r"([Pp][aá]gina\s+\d+\s*de\s*\d+|\s+|\x0c)", " ", plain_text)
            plain_text = re_sp.sub("", plain_text)
            if len(plain_text) == 0 or startswith(all_text, "$QH[R", "$1(;2", ""):
                return self.__load_pdf_ocr(file)
            return all_text

    def __load_pdf_ocr(self, file: Path):
        file_ocr = file.with_suffix(FileManager.OCR_SUFFIX)
        if file_ocr.exists():
            return self.load_txt(file_ocr)
        pages: list[str] = []
        pdf = fitz.open(file)

        for page in pdf:
            pix = page.get_pixmap(dpi=300)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            txt = image_to_string(img, lang="spa")
            pages.append(txt)
        content = "\n".join(pages)
        self.dump_txt(file_ocr, content)
        return content


def startswith(text: str, *prefixes: str) -> bool:
    for p in prefixes:
        if text.startswith(p):
            return True
    return False


# Mejoras dinamicas en la documentacion
for mth in dir(FileManager):
    slp = mth.split("_", 1)
    if len(slp) == 2 and slp[0] in ("load", "dump"):
        key, ext = slp
        mth = getattr(FileManager, mth)
        if mth.__doc__ is None:
            if key == "load":
                mth.__doc__ = "Lee "
            else:
                mth.__doc__ = "Guarda "
            mth.__doc__ = mth.__doc__ + "un fichero de tipo "+ext

FM = FileManager()
