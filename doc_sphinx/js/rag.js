// check URL if it is search.html
if (window.location.href.indexOf("search.html") > -1) {

    // get query string after ?q=
    const query = window.location.search.split("?q=")[1];

    // on document load event
    document.addEventListener("DOMContentLoaded", function () {
        // get the search results div
        const searchResults = document.getElementById("search-results");
        // add div before search results
        searchResults.insertAdjacentHTML("beforebegin",
            "<div id='ai-block' style='border: 1px solid var(--pst-color-border); border-radius: .25rem; margin: 1em 0; padding: 1em; display: none;'>" +
            "<h2 style='margin: 0; font-size: var(--pst-font-size-icon);'>AI-generated answer</h2>" +
            "<div style='color: var(--pst-color-text-muted); margin-bottom: 1em;'>May contain mistakes. Please check the references.</div>" +
            "<div id='rag-results' style='overflow: scroll; height: 20em;'></div></div>");

        addStream(query);
    });
}

function addStream(query) {
    const sse = new EventSource("/api/assistant?query=" + query);
    let chatData = "";

    sse.addEventListener("message", (e) => {
        // e.data is JSON string, so we need to parse it first
        chatData += JSON.parse(e.data);
        if (!(chatData.startsWith("Hmm, I'm not sure.") || "Hmm, I'm not sure.".startsWith(chatData))) {
            showRagResults(marked.parse(chatData));
        }
    });

    sse.addEventListener("error", (e) => {
        sse.close();
    });
}

function showRagResults(data) {
    /* show #ai-block */
    document.getElementById("ai-block").style.display = "block";

    // add the data to the div, only if it is not empty
    document.getElementById("rag-results").innerHTML = data;

    // replace all links with appropriate title
    $('#rag-results a').each(function (i, el) {
        var $el = $(el);
        var href = $el.attr('href');
        if (href.indexOf('https://docs.pip.distribution.sol.onetick.com/') > -1) {
            // remove the domain from the href
            var path = href.replace('https://docs.pip.distribution.sol.onetick.com/', '');
            path = path.replace('.html', '');
            var ind = Search._index.docnames.indexOf(path);
            if (ind > -1) {
                $el.html("[<b>" + Search._index.titles[ind] + "</b>]");
            }
        }
    })
}

function convertToTextarea() {
    /* TEMPORARY DISABLED: 
    this is convenient to have multiple lines in search box for AI (e.g. long questions with code),
    but this kind of dirty hack breaks some of theme behavior (e.g. mobile view, auto-focus on input, etc.)
    Kept for future purposes or as a history, to activate - uncomment "setTimeout(convertToTextarea, 0);" below */
    $("form.bd-search input").each(function () {
        const input = $(this).get(0);

        // Create a new textarea element
        const textarea = document.createElement('textarea');

        // Copy all attributes from input to textarea
        for (let attr of input.attributes) {
            textarea.setAttribute(attr.name, attr.value);
        }

        // Set the textarea value to the input value
        textarea.value = input.value;

        styles = "padding-left: 2.5rem; font-size: var(--pst-font-size-icon); height: calc(1.5em + .75rem + 2px);"
        textarea.setAttribute('style', styles);
        textarea.setAttribute('rows', '1');
        textarea.setAttribute('oninput', "this.style.height = ''; this.style.height = 'calc(2px + ' + this.scrollHeight + 'px)'");
        textarea.addEventListener('keydown', function (event) {
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault(); // Prevent the default newline behavior
                this.closest('form').submit(); // Submit the form
            }
        });

        // Replace the input with the textarea
        input.parentNode.replaceChild(textarea, input);
    });
}

$(document).ready(function () {
    $('.version-switcher__menu').css('max-height', '100vh').css('overflow-y', 'scroll');
    // setTimeout(convertToTextarea, 0);
});


function performVectorSearch(query) {
    // array of [filename, title, anchor, descr, score]
    var results = [];

    // AJAX request to the server
    $.ajax({
        url: '/api/ragsearch?query=' + query,
        type: 'GET',
        dataType: 'json',
        success: function (data) {
            results = data;
            displayNextItem();
        }
    });

    /* Full copy from searchtools.js for compatibility */
    function displayNextItem() {
        // results left, load the summary and display it
        if (results.length) {
            var item = results.pop();
            var listItem = $('<li></li>');
            var requestUrl = "";
            var linkUrl = "";
            if (DOCUMENTATION_OPTIONS.BUILDER === 'dirhtml') {
                // dirhtml builder
                var dirname = item[0] + '/';
                if (dirname.match(/\/index\/$/)) {
                    dirname = dirname.substring(0, dirname.length - 6);
                } else if (dirname == 'index/') {
                    dirname = '';
                }
                requestUrl = DOCUMENTATION_OPTIONS.URL_ROOT + dirname;
                linkUrl = requestUrl;

            } else {
                // normal html builders
                requestUrl = DOCUMENTATION_OPTIONS.URL_ROOT + item[0] + DOCUMENTATION_OPTIONS.FILE_SUFFIX;
                linkUrl = item[0] + DOCUMENTATION_OPTIONS.LINK_SUFFIX;
            }
            listItem.append($('<a/>').attr('href', linkUrl).html(item[1]));
            if (item[3]) {
                listItem.append($('<span> (' + item[3] + ')</span>'));
                Search.output.append(listItem);
                setTimeout(function () {
                    displayNextItem();
                }, 5);
            } else if (DOCUMENTATION_OPTIONS.SHOW_SEARCH_SUMMARY) {
                $.ajax({
                    url: requestUrl,
                    dataType: "text",
                    complete: function (jqxhr, textstatus) {
                        var data = jqxhr.responseText;
                        if (data !== '' && data !== undefined) {
                            var summary = Search.makeSearchSummary(data, [], []);
                            if (summary) {
                                listItem.append(summary);
                            }
                        }
                        Search.output.append(listItem);
                        setTimeout(function () {
                            displayNextItem();
                        }, 5);
                    }
                });
            } else {
                // just display title
                Search.output.append(listItem);
                setTimeout(function () {
                    displayNextItem();
                }, 5);
            }
        }
        // search finished, update title and status message
        else {
            Search.stopPulse();
            Search.title.text(_('Search Results'));
            // if (!resultCount)
            //     Search.status.text(_('Your search did not match any documents. Please make sure that all words are spelled correctly and that you\'ve selected enough categories.'));
            // else
            //     Search.status.text(_('Search finished, found %s page(s) matching the search query.').replace('%s', resultCount));
            Search.status.fadeIn(500);
        }
    }
}

function setVectorSearch() {
    // hacky way to override Search.query from original keyword based search
    Search.query = performVectorSearch;
}

$(document).ready(function () {
    fetch('/api_check').then(response => {
        if (response.ok) {
            console.log("API check passed, so using vector search");
            setVectorSearch();
        } else {
            console.log("API check failed, so using default search");
        }
    }).catch(e => {
        console.log("API check failed with error, so using default search");
        console.log(e)
    });
});
