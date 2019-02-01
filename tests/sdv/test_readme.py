from sdv import SDV


def test_readme_code():
    data_vault = SDV('tests/data/meta.json')
    data_vault.fit()
    samples = data_vault.sample_all()
    for dataset in samples:
        print(samples[dataset].head(3), '\n')


if __name__ == '__main__':
    test_readme_code()
