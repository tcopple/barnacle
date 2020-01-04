from unittest import TestCase

from barnacle.models.holding import HoldingFactory, ShareHolding, OptionHolding

"""                                                             VALUE  SHARES/ SH/ PUT/ INVSTMT  OTHER            VOTING AUTHORITY
NAME OF ISSUER                 TITLE OF CLASS   CUSIP     (x$1000)  PRN AMT PRN CALL DSCRETN MANAGERS         SOLE   SHARED     NONE
------------------------------ ---------------- --------- -------- -------- --- ---- ------- ------------ -------- -------- --------
3-D SYS CORP DEL               COM NEW          88554D205     1790    33546 SH       SOLE                    31346        0        0
AAR CORP                       NOTE 1.750% 2/0  000361AH8     2867  2860000 PRN      SOLE                        0        0        0
"""


class HoldingFactoryTests(TestCase):
    @classmethod
    def _make_context(cls, asof):
        context = PricingContext(asof)
        context.pricing_model = VannaVolgaSABR()
        context.pricing_type = OptionPricingType.BP
        context.redis_work = None
        context.use_calibration_hint_cache = False

        return context

    def setUp(self):
        self.SHARE_RECORD = {
            "nameOfIssuer": "3-D SYS CORP DEL",
            "titleOfClass": "COM NEW",
            "cusip": "88554D205",
            "value": "1790",
            "shrsOrPrnAmt": {"sshPrnamtType": "SH", "sshPrnamt": "33546"},
        }

        self.OPTION_RECORD = {
            "nameOfIssuer": "AAR CORP",
            "titleOfClass": "NOTE 1.750% 2/0",
            "cusip": "000361AH8",
            "value": "2867",
            "shrsOrPrnAmt": {"sshPrnamtType": "PRN", "sshPrnamt": "2860000"},
            "putCall": "put",
        }

    def test_should_make_share_holding_when_share_type_field_is_sh(self):
        holding = HoldingFactory.make(self.SHARE_RECORD)
        self.assertIsInstance(holding, ShareHolding)

    def test_should_make_option_holding_when_share_type_field_is_prn(self):
        holding = HoldingFactory.make(self.OPTION_RECORD)
        self.assertIsInstance(holding, OptionHolding)

    def test_should_raise_error_if_issuer_field_is_not_present(self):
        self.SHARE_RECORD.pop("nameOfIssuer")
        with self.assertRaises(Exception):
            HoldingFactory.make(self.SHARE_RECORD)

    def test_should_raise_error_if_title_of_class_field_is_not_present(self):
        self.SHARE_RECORD.pop("titleOfClass")
        with self.assertRaises(Exception):
            HoldingFactory.make(self.SHARE_RECORD)

    def test_should_raise_error_if_cusip_field_is_not_present(self):
        self.SHARE_RECORD.pop("cusip")
        with self.assertRaises(Exception):
            HoldingFactory.make(self.SHARE_RECORD)

    def test_should_raise_error_if_value_field_is_not_present(self):
        self.SHARE_RECORD.pop("value")
        with self.assertRaises(Exception):
            HoldingFactory.make(self.SHARE_RECORD)

    def test_should_raise_error_if_share_information_is_not_present(self):
        self.SHARE_RECORD.pop("shrsOrPrnAmt")
        with self.assertRaises(Exception):
            HoldingFactory.make(self.SHARE_RECORD)

    def test_should_scale_value_field_by_one_thousand(self):
        holding = HoldingFactory.make(self.SHARE_RECORD)
        self.assertEqual(holding.value, float(self.SHARE_RECORD.get("value")) * 1000.0)


class HoldingTests(TestCase):
    def test_foo(self):
        pass
