import unittest
from types import SimpleNamespace

import main
from telethon.tl.types import KeyboardButtonCallback


class ExtractionTests(unittest.TestCase):
    def setUp(self):
        self.bots = {"cryptobot", "send"}

    def test_explicit_cryptobot_link_is_not_repeated_for_default_bot(self):
        matches = main.extract_matches(
            "https://t.me/CryptoBot?start=CQabc123",
            self.bots,
        )

        self.assertEqual(matches, [("cryptobot", "CQabc123")])

    def test_path_style_link_is_not_repeated_for_default_bot(self):
        matches = main.extract_matches("t.me/CryptoBot/CQabc123", self.bots)

        self.assertEqual(matches, [("cryptobot", "CQabc123")])

    def test_raw_payload_uses_default_bot(self):
        self.assertEqual(
            main.extract_matches("new check CQabc123", self.bots),
            [("send", "CQabc123")],
        )


class ClaimButtonTests(unittest.TestCase):
    def test_extracts_receive_callback(self):
        message = SimpleNamespace(
            reply_markup=SimpleNamespace(
                rows=[
                    SimpleNamespace(
                        buttons=[KeyboardButtonCallback("💸 Получить…", b"claim-check")]
                    )
                ]
            )
        )

        self.assertEqual(
            main.extract_claim_callbacks(message),
            [("💸 Получить…", b"claim-check")],
        )

    def test_ignores_unrelated_callback(self):
        message = SimpleNamespace(
            reply_markup=SimpleNamespace(
                rows=[SimpleNamespace(buttons=[KeyboardButtonCallback("Кошелёк", b"wallet")])]
            )
        )

        self.assertEqual(main.extract_claim_callbacks(message), [])


class ClaimCallbackTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.original_client = main.client
        main.callback_seen.clear()

    async def asyncTearDown(self):
        main.client = self.original_client

    async def test_clicks_callback_only_once(self):
        requests = []

        class FakeClient:
            async def __call__(self, request):
                requests.append(request)
                return SimpleNamespace(message=None)

        main.client = FakeClient()
        message = SimpleNamespace(
            id=321,
            reply_markup=SimpleNamespace(
                rows=[SimpleNamespace(buttons=[KeyboardButtonCallback("Получить", b"claim")])]
            ),
        )

        first = await main.click_claim_button(message, "send", "peer", "CQabc123")
        second = await main.click_claim_button(message, "send", "peer", "CQabc123")

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].msg_id, 321)
        self.assertEqual(requests[0].data, b"claim")


class AliasDeduplicationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        main.seen.clear()
        while not main.claim_queue.empty():
            main.claim_queue.get_nowait()
            main.claim_queue.task_done()

    async def test_aliases_of_same_peer_queue_one_claim(self):
        message = SimpleNamespace(
            raw_text="https://t.me/CryptoBot?start=CQabc123",
            entities=[],
            buttons=[],
            chat_id=100,
            id=200,
        )

        await main.process_message(
            message,
            {"cryptobot": "send", "send": "send"},
        )

        item = main.claim_queue.get_nowait()
        self.assertEqual((item.bot, item.payload), ("send", "CQabc123"))
        self.assertTrue(main.claim_queue.empty())
        main.claim_queue.task_done()


if __name__ == "__main__":
    unittest.main()
