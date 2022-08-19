$(() => {
  const events = () => {
    const route = /localhost/.test(location.origin) ? '/' : '/weather/';

    $('span.server').text(location.origin + route);

    $('a:not([href])').attr('href', function() {
      return $(this).text();
    });

    $('#LatLon').change(function() {
      $('#Location').val('');
    });

    $('#Location').change(function() {
      $('#LatLon').val('');
    });

    $('#Playground select').focus(function() {
      $(this).change();
    });

    $('#Playground select').change(function() {
      let href = location.origin + route;
      let parms = [];
      let str = [];

      $('#Playground select option:checked').each(function() {
        let parm = $(this).data('parm');
        let focused = $(this).parent().is(':focus');

        if (parm) {
          parms.push(parm);
          if (focused) {
            str.push(`<span class="focused">${parm}</span>`);
          } else {
            str.push(parm);
          }
        }
      });

      $('#Playground a').html(href + str.join('&').replace('?&', '?'))[0].href = href + parms.join('&').replace('?&', '?');
    }).first().change();
  } // events

  const date1 = new Date(Date.now() - 12096e5);
  const date2 = new Date(Date.now() + 12096e5);
  $('.date1').text(dateFormat(date1, 'mmm dd, yyyy'));
  $('.date2').text(dateFormat(date2, 'mmm dd, yyyy'));
  $('.date1b').text(dateFormat(date1, 'yyyy-mm-dd'));
  $('.date2b').text(dateFormat(date2, 'yyyy-mm-dd'));

  events();
});