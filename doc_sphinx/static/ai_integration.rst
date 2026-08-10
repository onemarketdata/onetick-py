OneTick AI integration
======================

``onetick-py`` can be integrated with AI coding assistants such as Claude Code.

OneTick AI integration generally makes each prompt related to
OneTick or ``onetick-py`` better and faster and uses less tokens,
because it has access to documentation, tools and *skills* maintained by OneTick team.

This page covers two ways of AI integration: an MCP server and a *Skill*/Plugin.

Installation examples use `Claude <https://claude.ai/>`_, but may work for other AI providers too.

Connecting to OneTick MCP server
--------------------------------

`MCP <https://modelcontextprotocol.io/>`_ (Model Context Protocol) server
lets an AI assistant call tools exposed by this server instead of guessing.

OneTick MCP server expose tools helping
to write queries using ``onetick-py``, search documentation or to work with OneTick market data.

See `<https://code.claude.com/docs/en/mcp-quickstart>`_ for details.

Claude Code
~~~~~~~~~~~

Register a remote MCP server with the ``claude mcp add`` command in the terminal:

.. code-block:: bash

    claude mcp add --transport http onetick-mcp https://mcp.docs.sol.onetick.com/mcp


Verify the server is connected by running ``claude mcp list`` in the terminal
or ``/mcp`` inside a Claude Code session.

Claude Desktop
~~~~~~~~~~~~~~

Add the server to ``claude_desktop_config.json``:

.. code-block:: json

    {
      "mcpServers": {
        "onetick-mcp": {
          "type": "http",
          "url": "https://mcp.docs.sol.onetick.com/mcp"
        }
      }
    }

Restart Claude Desktop after editing the file for the server to show up.

Adding a OneTick Skills/Plugins
-------------------------------

A `Skill <https://docs.claude.com/en/docs/claude-code/skills>`_
is just a text file or a folder of instructions
that Claude Code use automatically when a task matches the skill's description.

.. only:: Internal

    For ``onetick-py`` this means the assistant gets the access
    to the ``onetick-py`` documentation and instructions covering popular use-cases and
    best practices of writing the ``onetick-py`` code.

    In Claude Skills are distributed as *plugins*.
    OneTick team maintains a Claude plugin *marketplace* that contains different skills.

    See `<https://code.claude.com/docs/en/discover-plugins#add-marketplaces>`_ for details.

    Install OneTick plugins from Claude Code session:

    .. code-block:: bash

        /plugin marketplace add git@gitlab.sol.onetick.com:solutions/ml-ops/onetick-coding-skills.git
        /plugin install onetick-py-coding@onetick-coding-skills
        /reload-plugins

    Once installed, the skill triggers automatically whenever
    you ask Claude to work with ``onetick-py`` code.

    Verify it with ``/plugin`` or ``/skills``
    or by asking an ``onetick-py`` question and watching Claude consult the bundled reference.

Writing your own skills
-----------------------

It's easy to write your own skills too.
It's useful if some of your everyday tasks and topics are not fully covered by
Skills provided by Claude or OneTick.

See `<https://code.claude.com/docs/en/skills#getting-started>`_ for details.

Create ``.claude/skills/my-onetick-py-skill/SKILL.md`` file:

.. code-block:: markdown

    ---
    name: my-onetick-py-skill
    description: >-
      Write, debug, or review onetick.py (otp) code for the OneTick tick database.
      Use whenever the task involves onetick.py, otp., otp.DataSource/otp.Ticks/otp.run,
      OneTick databases, or OneTick queries.
    ---

    # Instructions

    - Consult the onetick-py documentation before writing code from memory:
      https://docs.pip.distribution.sol.onetick.com/intro.html
    - Every query needs a tick type and database on `otp.DataSource`; never invent
      database or tick type names, list them with `otp.databases()` first.
    - Symbols and the query interval are set once, in `otp.run` (not scattered across
      the query) -- see the "Concepts" chapter for query interval and symbol binding rules.
    - After writing a query, run it with `otp.run(...)` and inspect the resulting
      `pandas.DataFrame` before presenting it -- most mistakes (empty results, wrong
      columns, schema errors) are only visible once the query actually runs.

Claude automatically scans ``~/.claude/skills`` or ``.claude/skills`` directories to discover skills.

Claude matches the ``description`` field against your prompts to decide when to load the skill.
If the description is too broad, one skill can overshadow the other,
so be specific when writing skill description.

Skill instructions can also use other skills or MCP servers,
so, for example, you don't need to repeat instructions covered by OneTick AI integration,
just let Claude know in which cases to use or not to use it.
