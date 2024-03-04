import prov.model

from provinspector.utils.prov_utils import document_factory, qualified_name


class TestProvUtils:
    def test_document_factory_and_qualified_name(self):
        doc = prov.model.ProvDocument(records=None)
        doc.set_default_namespace(
            uri="https://dbgit.prakinf.tu-ilmenau.de/masc7357/provinspector/"
        )

        # compare documents
        assert doc == document_factory()

        # compare namespaces
        assert doc.get_default_namespace() == qualified_name("").namespace
