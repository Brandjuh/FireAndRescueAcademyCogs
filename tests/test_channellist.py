import unittest

from channellist.channellist import (
    ACTION_EDIT,
    ACTION_REPOST,
    ACTION_SKIP,
    DEFAULT_EMOJI,
    EMPTY_LIST_PLACEHOLDER,
    MESSAGE_CHAR_LIMIT,
    chunk_blocks,
    decide_action,
    format_category_header,
    format_channel_line,
    normalize_topic,
    render_blocks,
)


class NormalizeTopicTests(unittest.TestCase):
    def test_none_and_empty(self):
        self.assertEqual(normalize_topic(None), "")
        self.assertEqual(normalize_topic(""), "")
        self.assertEqual(normalize_topic("   "), "")

    def test_collapses_whitespace_and_newlines(self):
        self.assertEqual(
            normalize_topic("Read this\nand you\t know   what to do."),
            "Read this and you know what to do.",
        )


class FormatLineTests(unittest.TestCase):
    def test_channel_line_with_topic(self):
        self.assertEqual(
            format_channel_line("<#1>", "Chillax at the hangout."),
            "<#1> - Chillax at the hangout.",
        )

    def test_channel_line_without_topic(self):
        self.assertEqual(format_channel_line("<#1>", None), "<#1>")
        self.assertEqual(format_channel_line("<#1>", "   "), "<#1>")

    def test_category_header(self):
        self.assertEqual(
            format_category_header("General Chat", DEFAULT_EMOJI),
            "**[⏬] [GENERAL CHAT] [⏬]**",
        )

    def test_category_header_without_emoji(self):
        self.assertEqual(format_category_header("Reception", ""), "**[RECEPTION]**")


class RenderBlocksTests(unittest.TestCase):
    def test_groups_headers_with_channels(self):
        blocks = render_blocks(
            [
                ("Reception", [("<#1>", "Welcome"), ("<#2>", None)]),
                ("Empty", []),
                (None, [("<#9>", "Top level")]),
            ],
            DEFAULT_EMOJI,
        )
        self.assertEqual(
            blocks,
            [
                ["**[⏬] [RECEPTION] [⏬]**", "<#1> - Welcome", "<#2>"],
                ["<#9> - Top level"],
            ],
        )


class ChunkBlocksTests(unittest.TestCase):
    def test_single_message_when_short(self):
        blocks = [["**[A]**", "<#1> - one"], ["**[B]**", "<#2> - two"]]
        chunks = chunk_blocks("Header", blocks)
        self.assertEqual(len(chunks), 1)
        self.assertTrue(chunks[0].startswith("Header"))
        self.assertIn("**[A]**", chunks[0])
        self.assertIn("**[B]**", chunks[0])

    def test_blocks_separated_by_blank_line(self):
        blocks = [["**[A]**", "<#1>"], ["**[B]**", "<#2>"]]
        chunks = chunk_blocks("", blocks)
        self.assertEqual(chunks[0], "**[A]**\n<#1>\n\n**[B]**\n<#2>")

    def test_splits_when_over_limit(self):
        blocks = [["<#{}> - {}".format(i, "x" * 100)] for i in range(60)]
        chunks = chunk_blocks("Header", blocks)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk), MESSAGE_CHAR_LIMIT)

    def test_header_not_stranded_at_message_bottom(self):
        # Fill the first message almost to the limit, then add a category whose
        # header would land alone at the bottom.
        filler = ["<#0> - " + "a" * 100 for _ in range(18)]
        blocks = [filler, ["**[NEXT]**", "<#99> - channel"]]
        chunks = chunk_blocks("", blocks)
        for chunk in chunks:
            if "**[NEXT]**" in chunk:
                self.assertIn("<#99> - channel", chunk)


class DecideActionTests(unittest.TestCase):
    def test_repost_when_nothing_posted(self):
        self.assertEqual(decide_action(["a"], None), ACTION_REPOST)
        self.assertEqual(decide_action(["a"], []), ACTION_REPOST)

    def test_repost_when_more_messages_needed(self):
        self.assertEqual(decide_action(["a", "b"], ["a"]), ACTION_REPOST)

    def test_skip_when_identical(self):
        self.assertEqual(decide_action(["a", "b"], ["a", "b"]), ACTION_SKIP)

    def test_edit_when_content_differs_same_count(self):
        self.assertEqual(decide_action(["a", "c"], ["a", "b"]), ACTION_EDIT)

    def test_edit_when_fewer_messages_needed(self):
        self.assertEqual(decide_action(["a"], ["a", "b"]), ACTION_EDIT)


class PlaceholderTests(unittest.TestCase):
    def test_placeholder_constant(self):
        self.assertTrue(EMPTY_LIST_PLACEHOLDER)


if __name__ == "__main__":
    unittest.main()
