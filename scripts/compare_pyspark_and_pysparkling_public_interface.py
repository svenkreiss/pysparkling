from importlib import import_module
import logging
from pathlib import Path

import pyspark

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger()


pyspark_root = Path(pyspark.__file__).parent
pysparkling_root = Path('../pysparkling')

examples = pyspark_root / 'examples'

files_to_compare = [
    module
    for module in pyspark_root.rglob('*.py')
    if examples not in module.parents
]


files_to_compare = sorted(files_to_compare)


def tell_files_to_be_internalized():
    log.info('# Files that should not be exposed:')

    pysparkling_files = {
        file.relative_to(pysparkling_root)
        for file in pysparkling_root.rglob('*.py')
        if (
                not file.name.startswith('_')
                and not any(parent.name.startswith('_') for parent in file.parents)
                and not any('tests' in x.name for x in file.parents)
        )
    }

    pyspark_files = {
        file.relative_to(pyspark_root)
        for file in files_to_compare
        if (
            not file.name.startswith('_')
            and not any(parent.name.startswith('_') for parent in file.parents)
        )
    }

    print(pysparkling_files)
    print(pyspark_files)

    for file in sorted(pysparkling_files - pyspark_files):
        log.info('- %s', file)




def compare_with_module(pysparkling_path, converted_to_module_name, pyspark_mod):
    pysparkling_module_name = 'pysparkling' + converted_to_module_name[7:]

    try:
        pysparkling_mod = import_module(pysparkling_module_name)
    except ImportError:
        log.error(f" --> CANNOT LOAD %s", pysparkling_module_name)
        return

    pyspark_vars = set(vars(pyspark_mod))
    pysparkling_vars = set(vars(pysparkling_mod))

    if '__all__' in pyspark_vars:
        pyspark_all = set(pyspark_mod.__all__)

        if '__all__' not in pysparkling_vars:
            log.warning('  __all__ is not defined in pysparkling! Going to check the symbols anyway.')
            fake_all = True
            pysparkling_all = set(vars(pysparkling_mod))
        else:
            fake_all = False
            pysparkling_all = set(pysparkling_mod.__all__)

        if pysparkling_all != pyspark_all:
            if not fake_all:
                log.warning('  __all__ is not the same:')

            if pyspark_all - pysparkling_all:
                log.warning('    pysparkling is still missing:')
                for x in sorted(pyspark_all - pysparkling_all):
                    log.warning('    - %s', x)
            elif fake_all:
                log.info("    Just add ```__all__ = %s```", pyspark_mod.__all__)

            if not fake_all and pysparkling_all - pyspark_all:
                log.warning('    pysparkling has these too much:')
                for x in sorted(pysparkling_all - pyspark_all):
                    log.warning('    - %s', x)
            return

        log.info("  ==> All ok")
        return

    if pyspark_vars - pysparkling_vars:
        log.warning('    pysparkling is still missing:')
        for x in sorted(pyspark_vars - pysparkling_vars):
            log.warning('    - %s', x)

    if pysparkling_vars - pyspark_vars:
        log.warning('    pysparkling has these too much:')
        for x in sorted(pysparkling_vars - pyspark_vars):
            log.warning('    - %s', x)


def tell_differences_between_modules():
    log.info('# REPORT: pyspark vs pysparkling')

    for file in files_to_compare:
        relative_path = file.relative_to(pyspark_root.parent)

        converted_to_module_name = (
            str(relative_path)[:-3]  # Strip .py
            .replace('\\', '/')      # Windows paths?
            .replace('/', '.')       # --> module name
        )

        log.info('* %s', converted_to_module_name)
        try:
            mod = import_module(converted_to_module_name)
        except ImportError:
            log.error(f" --> CANNOT IMPORT %s", converted_to_module_name)
            continue

        pysparkling_path = pysparkling_root / file.relative_to(pyspark_root)

        compare_with_module(
            pysparkling_path,
            converted_to_module_name,
            mod
        )


if __name__ == '__main__':
    tell_files_to_be_internalized()
    tell_differences_between_modules()
