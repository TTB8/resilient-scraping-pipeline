/* Test stand-in for the jQuery file the captured /js/ pages load (it was not captured).
   It provides only $.map, the one helper their inline script calls. Not jQuery. */
(function () {
  function map(items, callback) {
    var out = [];
    for (var i = 0; i < items.length; i++) {
      var value = callback(items[i], i);
      if (value !== null && value !== undefined) {
        out.push(value);
      }
    }
    return out;
  }
  window.jQuery = window.$ = { map: map };
})();
