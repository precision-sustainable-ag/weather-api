// Rick's Poor Man's jQuery, plus supporting functions

let fy = '2223';

Array.prototype.get = function() {return this;}  // PMJ's .map automatically does .get, but this allows it to work with legacy jQuery code

// jQuery START
const jqMore = {};

const $ = (() => {
  let iframe;
  
  const elemDisplay = {};

  const $ = (selector, context) => {
    if (typeof selector == 'function') {
      if (/complete|interactive/.test(document.readyState)) {
        selector();
      } else {
        window.addEventListener('DOMContentLoaded', selector);  
      }
    } else {
      const html = /</.test(selector);
      let tp;

      if (html) {
        tp = document.createElement('template');
        tp.innerHTML = '<div class="MAIN">' + selector + '</div>';
        // $('body').append(selector);
      }

      let r;

      const contextIsJq = context && context[0] && context[0].nodeName;
      let contextIsObject = typeof context == 'object' && !contextIsJq && !context.nodeName;

      try {
        r = Array.isArray(selector)               ? selector :
            typeof selector == 'object'           ? [selector] :
            html                                  ? [...tp.content.querySelectorAll('.MAIN > *')] :
            context && !contextIsObject           ? [...$(context)[0].querySelectorAll(selector)] :
                                                    [...document.querySelectorAll(selector)];
      } catch(e) {
        console.log(selector);
        console.log(e.message);
      }

      if (html) {
        // console.log(r.length);
      }

      const fe = (fnc) => {
        r.forEach((obj, i) => fnc.bind(obj, obj, i)());
        return r;
      }

      const unique = (collection) => $([...new Set(collection.filter(obj => obj))]);

      const ud = (parm) => parm === undefined;

      const matches = (obj, selector) => obj && (!selector || obj.matches(selector)) && obj;

      const unitize = (value) => {
        if (value !== undefined && /^[\d.]+$/.test(value.toString())) {
          value += 'px';
        }
        return value;
      } // unitize

      if (contextIsObject) {
        fe(obj => {
          OK(context).forEach(key => {  // TODO Something's wrong here, for example $('td:nth-child(1)', $tn);
            obj.setAttribute(key, context[key])
            obj[key.replace(/^class$/, 'className')] = context[key];
          });
        });
      }

      r.clone = () => $([...r].map(obj => obj.cloneNode(true)));

      r.has = (selector) => $([...r].filter(e => typeof selector == 'object' ? e === selector : e.querySelector(selector)));

      r.is = (selector) => [...r].some(obj => selector === ':visible' || obj.matches(selector));  // TODO  :visible needed for chosen

      r.map = (callback) => [...r].map((obj, i) => callback.bind(obj, i, obj)());

      r.not = (selector) => $([...r].filter(obj => typeof selector == 'object' ? obj !== selector : !obj.matches(selector)));

      r.empty = () => fe(obj => obj.textContent = '');

      r.remove = () => fe(obj => obj.remove());

      r.addClass = (className) => fe((obj, i) => {
        let cName = typeof className == 'function' ? className.bind(obj, i, obj.className)() : className;

        if (cName) {
          obj.classList.add(...cName.split(' '));
        }
      });

      r.removeClass = (className) => fe((obj, i) => {
        let cName = typeof className == 'function' ? className.bind(obj, i, obj.className)() : className;

        if (cName) {
          obj.classList.remove(...(cName || '').trim().split(/\s+/));
        } else {
          obj.className = '';
        }
      });

      r.hasClass    = (className) => [...r].some(obj => obj.classList.contains(className));

      r.toggleClass = (className, state) => fe(obj => obj.classList.toggle(className, state));

      r.toggle = (display) => fe(obj => display ? $(obj).show() : 
                              display === false ? $(obj).hide() :
                              getComputedStyle(obj).getPropertyValue('display') == 'none' ? $(obj).show() : $(obj).hide());

      r.hide = () => fe(obj => {
        let display = getComputedStyle(obj).getPropertyValue('display');

        if (display != 'none') {
          obj.defaultDisplay = display;
          obj.style.display = 'none';
          obj.classList.add('hidden');
        }
      });

      r.show = () => fe(obj => {
        let display = getComputedStyle(obj).getPropertyValue('display');

        if (display == 'none') {
          obj.classList.remove('hidden');
          const cname = obj.className;
          obj.className = '';
          
          if (obj.defaultDisplay || elemDisplay[obj.tagName]) {
            display = obj.defaultDisplay || elemDisplay[obj.tagName];
          } else {
            obj.style.display = '';
            
            if (!iframe) {
              iframe = document.createElement('iframe');
              iframe.style.display = 'none';
              document.body.appendChild(iframe);
            }

            const doc = iframe.contentDocument;
            const el = document.createElement(obj.tagName);
            doc.body.appendChild(el);  // this handles TR, TD, etc., even without required parent
            display = getComputedStyle(doc.body.childNodes[0]).getPropertyValue('display');
          }
          obj.className = cname;
          obj.style.display = obj.defaultDisplay = elemDisplay[obj.tagName] = display;
        }
      });

      r.hidden = () => unique([...r].filter(obj => !obj.offsetWidth && !obj.offsetHeight && !/^(BR)$/.test(obj.tagName)));
      r.visible = () => unique([...r].filter(obj => obj.offsetWidth && obj.offsetHeight));

      r.closest = (selector) => unique([...r].map(obj => obj.closest(selector)));

      r.parent  = (selector) => unique([...r].map(obj => matches(obj.parentElement, selector)));

      r.parents = (selector) => {
        const collection = [];

        r.forEach(obj => {
          while (obj = obj.parentNode) {
            if (obj.tagName && matches(obj, selector)) {
              collection.push(obj);
            }
          }
        });

        return unique(collection);
      } // parents

      r.children = (selector) => {  // !!! may not be working???
        const collection = [];

        r.forEach(obj => {
          [...obj.children].forEach(child => collection.push(matches(child, selector)))
        });

        return unique(collection);
      } // children

      r.siblings = (selector) => {
        const collection = [];

        r.forEach(obj1 => {
          [...obj1.parentNode.children]
            .filter(obj2 => obj1 != obj2 && matches(obj2, selector))
            .forEach(obj => collection.push(matches(obj, selector)))
        });

        return unique(collection);
      } // siblings

      r.slice = (start, end) => unique([...r].slice(start, end));

      r.width = (value) => {
        value = unitize(value);

        return ud(value)                  ? r[0] ? 
                                              r[0] == window ? innerWidth :
                                              parseInt(getComputedStyle(r[0]).getPropertyValue('width')) : 
                                                null :
               typeof value == 'function' ? fe((obj, i) => obj.style.width = value.bind(obj, i, obj.clientWidth)() + 'px') :
                                            fe(obj => obj.style.width = value);
      }

      r.outerHeight = (value) => {  // !!! includeMargin and function arguments may be needed.  Currently just duplicates height
        value = unitize(value);

        return ud(value)                  ? r[0] ? parseInt(getComputedStyle(r[0]).getPropertyValue('height')) : null :
               typeof value == 'function' ? fe((obj, i) => obj.style.height = value.bind(obj, i, obj.clientHeight)() + 'px') :
                                            fe(obj => obj.style.height = value);
      } // outerWidth

      r.outerWidth = (value) => {  // !!! includeMargin and function arguments may be needed.  Currently just duplicates width
        value = unitize(value);

        return ud(value)                  ? r[0] ? parseInt(getComputedStyle(r[0]).getPropertyValue('width')) : null :
               typeof value == 'function' ? fe((obj, i) => obj.style.width = value.bind(obj, i, obj.clientWidth)() + 'px') :
                                            fe(obj => obj.style.width = value);
      } // outerWidth

      r.height = (value) => {
        value = unitize(value);

        return ud(value)                  ? r[0] ? 
                                              r[0] == window ? innerHeight : 
                                              parseInt(getComputedStyle(r[0]).getPropertyValue('height')) :
                                                null :
               typeof value == 'function' ? fe((obj, i) => obj.style.height = value.bind(obj, i, obj.clientHeight)() + 'px') :
                                            fe(obj => obj.style.height = value);
      }

      r.find = (selector) => {
        const result = new Set();
        r.forEach(obj => obj.querySelectorAll(selector).forEach(o => result.add(o)));
        return $([...result]);
      }

      r.val  = (s) => ud(s) && r[0] && r[0].tagName == 'SELECT' && r[0].multiple ? [...r[0].querySelectorAll('option:checked')].map(option => option.value) :
                      ud(s)                   ? r[0] && r[0].value :
                      typeof s == 'function'  ? fe((obj, i) => obj.value = s.bind(obj, i, obj.value)()) :
                                                fe(obj => {
                                                  // if (obj.tagName === 'SELECT' && obj.prop('multiple')) {
                                                  //   alert('ok');
                                                  // }
                                                  return obj.value = s;
                                                });

      r.html = (s) => ud(s)                   ? (r[0] && r[0].innerHTML) || '' :
                      typeof s == 'function'  ? fe((obj, i) => obj.innerHTML = s.bind(obj, i, obj.innerHTML)()) :
                                                fe(obj => obj.innerHTML = s);

      r.text = (s) => ud(s)                   ? [...r].map(obj => obj.textContent).join('') :
                      typeof s == 'function'  ? fe((obj, i) => obj.textContent = s.bind(obj, i, obj.textContent)()) :
                                                fe(obj => obj.textContent = s);

      r.data = (key, value) => ud(value) ? r[0] && r[0].dataset[key] : fe(obj => obj.dataset[key] = value);

      r.attr = (key, value) => ud(value)                   ? r[0] && r[0].getAttribute(key) :
                               typeof value == 'function'  ? fe((obj, i) => obj.setAttribute(key, value.bind(obj, i, obj.getAttribute(key))())) :
                                                             fe(obj => obj.setAttribute(key, value));

      r.prop = (key, value) => ud(value)                   ? r[0] && r[0][key] :
                               typeof value == 'function'  ? fe(obj => obj[key] = value.bind(obj, i, obj[key])()) :
                                                             fe(obj => obj[key] = value);

      r.removeAttr = (key) => fe(obj => obj.removeAttribute(key));

      r.eq = (index) => index < 0 ? $(r[r.length - index - 2]) : $(r[index]);

      r.index = (selector) => {
        if (selector === undefined) {
          if (!r[0]) {
            return -1;
          } else {
            return [...r[0].parentNode.children].indexOf(r[0]);
          }        
        } else {
          alert('TODO: index');
        }
      } // index

      r.each = (fnc) => {r.forEach((obj, i) => fnc.bind(obj)(i, obj)); return r;}

      r.append  = (content) => {
        return fe(obj => {
          if (content !== undefined) {
            if (content[0] && content[0].nodeType == 1) {
              obj.append(content[0]);
            } else {
              if (obj.tagName == 'TABLE' && !/tbody/i.test(content)) {
                let tbody = obj.querySelector('tbody');
                if (!tbody) {
                  obj.insertAdjacentHTML('beforeend', '<tbody></tbody>');
                  tbody = obj.querySelector('tbody');
                }

                tbody.insertAdjacentHTML('beforeend', content);
              } else {
                obj.insertAdjacentHTML('beforeend', content);
              }
            }
          }
        });
      }

      r.prepend = (content) => {
        return fe(obj => {
          if (content !== undefined) {
            if (content[0] && content[0].nodeType == 1) {
              obj.prepend(content[0]);
            } else {
              if (obj.tagName == 'TABLE' && !/tbody/i.test(content)) {
                let tbody = obj.querySelector('tbody');
                if (!tbody) {
                  obj.insertAdjacentHTML('beforeend', '<tbody></tbody>');
                  tbody = obj.querySelector('tbody');
                }

                tbody.insertAdjacentHTML('afterbegin', content);
              } else {
                obj.insertAdjacentHTML('afterbegin', content)
              }
            }
          }
        });
      }

      r.appendTo = (target) => {
        target = $(target)[0];
        return fe(obj => target.append(obj));
      } // appendTo

      r.prependTo = (target) => {
        target = $(target)[0];
        return fe(obj => target.prepend(obj));
      } // appendTo

      r.next = (selector) => unique([...r].map(obj => matches(obj.nextElementSibling, selector)));

      r.nextAll = (selector) => {
        const collection = [];

        r.forEach(obj => {
          while (obj = obj.nextElementSibling) {
            collection.push(matches(obj, selector));
          }
        });

        return unique(collection);
      } // nextAll

      r.nextUntil = (selector) => {
        const collection = [];

        r.forEach(obj => {
          while ((obj = obj.nextElementSibling) && !matches(obj, selector)) {
            collection.push(obj);
          }
        });

        return unique(collection);
      } // nextUntil

      r.prev = (selector) => unique([...r].map(obj => matches(obj.previousElementSibling, selector)));

      r.prevAll = (selector) => {
        const collection = [];

        r.forEach(obj => {
          while (obj = obj.previousElementSibling) {
            collection.push(matches(obj, selector));
          }
        });

        return unique(collection);  // was collection.reverse(), but that fails the tester
      } // prevAll

      r.prevUntil = (selector) => {
        const collection = [];

        r.forEach(obj => {
          while ((obj = obj.previousElementSibling) && !matches(obj, selector)) {
            collection.push(obj);
          }
        });

        return unique(collection);
      } // prevUntil

      r.first = () => $(r[0]);

      r.last = () => $(r[r.length - 1]);

      r.trigger = (name) => {
        return fe(obj => obj.dispatchEvent(new Event(name, {bubbles: true})));
      }

      r.off = (type, selector, handler) => {
        type.split(' ').forEach(type => {
          if (handler) {
            fe(obj => {
              if (obj.handlers) {
                obj.removeEventListener(type, obj.handlers[type + selector]);
                delete obj.handlers[type + selector]
              }
            });
          } else {
            fe(obj => {
              if (obj.handlers) {
                obj.removeEventListener(type, obj.handlers[selector]);
                delete obj.handlers[selector];
              }
            });
          }
        });
        
        return r;
      } // off

      r.on = (type, selector, handler) => {
        type.split(' ').forEach(type => {
          if (handler) {
            fe(obj => {
              const h = (e) => {
                let nobj = e.target;

                while (nobj && nobj != obj) {
                  if (nobj.matches(selector)) {
                    if (handler.bind(nobj, e)() === false) {
                      e.preventDefault();
                      e.stopPropagation();
                    }
                    break;
                  }
                  nobj = nobj.parentNode;
                };
              } // h

              obj.handlers = obj.handlers || {};

              if (!obj.handlers[type + selector]) {
                obj.handlers[type + selector] = h;
                obj.addEventListener(type, h, /focus|blur/.test(type));
              }
            });
          } else {
            fe(obj => {
              obj.handlers = obj.handlers || {};

              if (!obj.handlers[type]) {
                obj.handlers[type] = selector;
                if (/\./.test(type)) {
                  const plugin = type.split('.').slice(1).join('.');  // unused?

                  type = type.split('.')[0];

                  obj.addEventListener(type, function(e) {
                    // alert(selector);
                    // alert(plugin);
                    if (selector.bind(obj, e)() === false) {
                      e.preventDefault();
                      e.stopPropagation();
                    }
                  });
                } else {
                  obj.addEventListener(type, function(e) {
                    if (selector.bind(obj, e)() === false) {
                      e.preventDefault();
                      e.stopPropagation();
                    }
                  });
                }
              }
            });
          }
        });

        return r;
      } // on

      r.fadeOut = () => fe(obj => $(obj).css({width: 0, height: 0, opacity: 0, transition: 'all 0.5s'}));  // !!! width and height ain't happening
      r.fadeIn  = () => fe(obj => $(obj).css({width: 'auto', height: 'auto', opacity: 1, transition: 'all 0.5s'}));

      r.css = (property, value) => {
        const setStyle = (obj, property, value) => {
          property = property.replace(/[A-Z]/, s => '-' + s.toLowerCase());

          // from jQuery source:
          // if (!/background|color|columnCount|fillOpacity|flexGrow|flexShrink|fontWeight|lineHeight|opacity|order|orphans|widows|zIndex|zoom/i.test(property)) {
          if (!/background|color|column-count|fill-opacity|flex-grow|flex-shrink|font-weight|line-height|opacity|order|orphans|widows|z-index|zoom/i.test(property)) {  
            value = unitize(value);
          }

          obj.style[property] = value;
        } // setStyle

        if (typeof property == 'object') {
          Object.keys(property).forEach(key => {
            fe(obj => setStyle(obj, key, property[key]));
          });
          return r;
        } else {
          property = property.replace(/[A-Z]/, s => '-' + s.toLowerCase());
          return ud(value) ? r[0] && getComputedStyle(r[0]).getPropertyValue(property) : fe(obj => setStyle(obj, property, value));
        }
      }

      r.filter = (selector) => {  // My extension of filter to deal with RegExp
        if (selector instanceof RegExp) {
          return $([...r].filter(obj =>  selector.test(obj.textContent)));
        } else {
          return $([...r].filter((obj, i) =>  selector.bind(obj, i, obj)()));
        }
      } // filter

      const insert = (type, target) => {
        const t = isHTML(target) ? target : $(target)[0];

        if (t) {
          return fe(obj => t[type](obj));
        } else {
          console.log(t);
        }
      } // insert

      const isHTML = s => /</.test(s);

      const ba = (where, content) => {
        if (typeof content == 'function') {
          return fe((obj, i) => $(content.bind(obj, i, obj)())['insert' + where](obj));
          // alert('TODO: ' + where.toLowerCase())
        } else if (isHTML(content)) {
          where = where.replace('Before', 'beforebegin').replace('After', 'afterend');
          return fe(obj => obj.insertAdjacentHTML(where, content));
        } else {
          return fe(obj => $(content)['insert' + where](obj));
        }
      }

      r.before = (content) => ba('Before', content);

      r.after = (content) => ba('After', content);

      r.insertBefore = (target) => insert('before', target);

      r.insertAfter  = (target) => insert('after', target);

      r.scrollLeft = (value) => {
        return ud(value) ? isFinite(r[0].scrollLeft) ? r[0].scrollLeft : r[0].scrollX :
                           fe(obj => obj.scrollLeft = value);
      }

      r.scrollTop = (value) => {
        return ud(value) ? isFinite(r[0].scrollTop) ? r[0].scrollTop : r[0].scrollY :
                           fe(obj => obj.scrollTop = value);
      }

      // console.log(JSON.stringify(Object.keys(r).sort()));

      Object.keys(jqMore).forEach(key => {
        // console.log('here', key, jqMore[key]);
        r[key] = jqMore[key]
      });

      r.END = () => null;  // unused except for DMS

      ['blur', 'change', 'click', 'contextmenu', 'dblclick', 'error', 'focus', 'focusout', 'input', 'keydown', 'keyup', 'keypress', 'load', 'mousedown', 'mousemove', 'mouseout', 'mouseover', 'mouseup', 'resize', 'scroll', 'select', 'submit', 'unload']
        .forEach(type => r[type] = (handler) => {
          if (handler) {
            r.forEach(obj => obj.addEventListener(type, function(e) {
              if (handler.bind(obj, e)() === false) {
                e.preventDefault();
                e.stopPropagation();
              }
            }));
          } else {
            if (/focus|submit/.test(type)) {
              r[0] && r[0][type]();
            } else {
              $(r[0]).trigger(type);
            }
          }
          return r;
        });

      return r;
    }
  } // $

  return $;
})();

$.trim = str => str.trim();

$.get = (url, callback) => {
  fetch(url).then(response => response.text()).then(data => callback && callback(data));

  return {
    fail: () => null  // silent fail until I figure this out
  }
} // $.get

$.post = (url, data, callback) => {
  $.ajax({
    url: url,
    method: 'POST',
    data: data,
    success: (data) => {
      callback(data);
    }
  });
} // $.post

$.getJSON = (url, callback) => fetch(url).then(response => response.json()).then(data => callback && callback(data));

$.getScript = (url, callback) => {
  $.get(url, src => {
    const script = document.createElement('script');
    script.innerHTML = src;
    document.head.appendChild(script);
    if (callback) {
      callback();
    }
  });
} // $.getScript

$.ajax = (options) => {
  options.body = Object.entries(options.data).map(([key, value]) => key + '=' + encodeURIComponent(value)).join('&');
  options.headers = {'Content-Type': 'application/x-www-form-urlencoded'};

  fetch(options.url, options)
    .then(response => response.text())
    .then(data => options.success(data))
    .catch(error => console.error('Error:', error));
} // $.ajax

$.contains = (container, contained) => container.contains(contained);

const jQuery = $;

jQuery.fn = jQuery.prototype = {
  jquery: '3.3',
  constructor: jQuery,
};

// Needed for Bootstrap:
  $.event = {special: {}};
  window.jQuery = jQuery;

$.extend = $.fn.extend = (target, ...rest) => {
  if (!rest.length) { // extend jQuery
    Object.keys(target).forEach(key => {
      // console.log(key, target[key]);
      jqMore[key] = target[key];
    });
    // Object.keys(target).forEach(key => Object.assign(target[key], jqMore));
    // console.log(jqMore.chosen);
    // console.log($.r)
  } else {
    rest.forEach(obj => Object.assign(target, obj));
  }
  
  return target;
} // $.extend

//  !!! Copied from jquery-2.1.4.js
jQuery.fn.extend({
  offset: function( options ) {
    if ( arguments.length ) {
      return options === undefined ?
        this :
        this.each(function( i ) {
          jQuery.offset.setOffset( this, options, i );
        });
    }

    var docElem, win,
      elem = this[ 0 ],
      box = { top: 0, left: 0 },
      doc = elem && elem.ownerDocument;

    if ( !doc ) {
      return;
    }

    docElem = doc.documentElement;

    // Make sure it's not a disconnected DOM node
    if ( !jQuery.contains( docElem, elem ) ) {
      return box;
    }

    // Support: BlackBerry 5, iOS 3 (original iPhone)
    // If we don't have gBCR, just use 0,0 rather than error
    var strundefined = typeof undefined;
    if ( typeof elem.getBoundingClientRect !== strundefined ) {
      box = elem.getBoundingClientRect();
    }
    // win = getWindow( doc );
    win = window;
    return {
      top: box.top + win.pageYOffset - docElem.clientTop,
      left: box.left + win.pageXOffset - docElem.clientLeft
    };
  },

  position: function() {
    if ( !this[ 0 ] ) {
      return;
    }

    var offsetParent, offset,
      elem = this[ 0 ],
      parentOffset = { top: 0, left: 0 };

    // Fixed elements are offset from window (parentOffset = {top:0, left: 0}, because it is its only offset parent
    if ( $(elem).css('position') === "fixed" ) {
      // Assume getBoundingClientRect is there when computed position is fixed
      offset = elem.getBoundingClientRect();

    } else {
      // Get *real* offsetParent
      offsetParent = this.offsetParent();

      // Get correct offsets
      offset = this.offset();
      if (offsetParent[0].nodeName != 'HTML') {
        parentOffset = offsetParent.offset();  // !!!
      }

      // Add offsetParent borders
      // parentOffset.top += jQuery.css( offsetParent[ 0 ], "borderTopWidth", true );
      // parentOffset.left += jQuery.css( offsetParent[ 0 ], "borderLeftWidth", true );
      parentOffset.top  += parseInt($(offsetParent[0]).css('borderTopWidth' ) || 0);
      parentOffset.left += parseInt($(offsetParent[0]).css('borderLeftWidth') || 0);
    }

    // Subtract parent offsets and element margins
    return {
      // top : offset.top  - parentOffset.top  - jQuery.css( elem, "marginTop", true ),
      // left: offset.left - parentOffset.left - jQuery.css( elem, "marginLeft", true )
      top  : offset.top  - parentOffset.top  - parseInt($(elem).css('marginTop' ) || 0),
      left : offset.left - parentOffset.left - parseInt($(elem).css('marginLeft') || 0)
    };
  },

  offsetParent: function() {
    return $(this.map(function() {
      var offsetParent = this.offsetParent || docElem;

      while ( offsetParent && ( offsetParent.nodeName != "html" && $( offsetParent).css("position" ) === "static" ) ) {
        offsetParent = offsetParent.offsetParent;
      }

      return offsetParent || document.documentElement;
    }));
  }
});

// jQuery END

// Useful Math functions ____________________________________________________________________________________________________________________________

Math.rpd = (v1, v2) => Math.abs(v1 - v2) / ((v1 + v2) / 2) * 100;

Math.rmse = (a1, a2) => a1.length ? Math.sqrt(a1.reduce((acc, n, i) => (acc + (n - a2[i]) ** 2), 0) / a1.length) : null;

Math.mean = (array) => array.length ? array.reduce((a, b) => +a + +b) / array.length : null;

Math.median = (array) => {
  array = array.sort((a, b) => a - b);

  const  middle = Math.floor((array.length - 1) / 2);
  
  if (array.length % 2) {
    return +array[middle];
  } else {
    return (+array[middle] + +array[middle + 1]) / 2.0;
  }
} // median

Math.stdDev = (array) => {
  const mean = Math.mean(array);
  const dev = array.map(itm => (itm - mean) * (itm - mean));

  return Math.sqrt(dev.reduce((a, b) => a + b) / (array.length - 1));
} //stdDev

Math.nse = (obs, sim) => {  // Nash-Sutcliffe Coefficient
  const avg = Math.mean(obs);
  const num = obs.reduce((acc, n, i) => (acc + (n - sim[i]) ** 2), 0);
  const den = obs.reduce((acc, n, i) => (acc + (n - avg   ) ** 2), 0);

  let acc = 0;
  obs.forEach((n, i) => acc += (n - sim[i]) ** 2);
  return 1 - num / den;
} // nse

Number.prototype.commas = function() {
  return this.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, "$1,");
}

// Misc. functions ____________________________________________________________________________________________________________________________

const queryStrings = () => {
  const searchParams = new URLSearchParams(location.search);

  const qs = {};

  for (const [key, value] of searchParams) {
    qs[key] = value;
  }

  return qs;
} // queryStrings

const OK = Object.keys;

Array.prototype.move = function(from, to) {
  this.splice(to, 0, this.splice(from, 1)[0]);
};

const fyArray = (start, end) => {
  const s = [];

  if (start < 0) {
    start = +fy + 101 * (start + 1);
    end = fy;
  } else if (start < 100) {
    start = (start - 1).toString().padStart(2, '0') + start.toString().padStart(2, '0');
    end   = (end   - 1).toString().padStart(2, '0') + end  .toString().padStart(2, '0');
  }

  for (let i = +start; i != +end; i += start < end ? 101 : -101) {
    s.push(i.toString().padStart(4, '0'));
  }

  s.push(end);
  
  return s;
} // fyArray

const saveFile = (fname, data, fnc) => {
  $.ajax({
    method: 'POST',
    url: '/_nr_save',
    data: {
      fn: fname,
      data: data
    }
  }).done(function() {
    if (fnc) {
      fnc();
    }
  });
} // saveFile

const getHits = (context) => {
  $.get('/_nr_ip', x => {
    PQ(
      'Hits',
      {IP          : x,
       DESCRIPTION : document.title,
       URL         : window.location.toString(),
       CONTEXT     : (context || '').replace(/"/g, '""'),
       DATE        : dateFormat('mm/dd/yyyy HH:MM')
      }
    );
  });
} // getHits

const View = (flds, data) => {
  const r = {};

  if (!flds) {
    r.error = true;
    return;
  }
  
  r.raw = data;

  r.row = 0;
  r.col = 0;

  r.rows = data.length;
  r.cols = flds.length;
  r.f = /./;

  let moving;

  const moveto = (ncol, nrow, scrolling) => {
    if (moving) {
      return false;
    } else if (!r.rowHeight) {
      setTimeout(() => moveto(ncol, nrow, scrolling), 10);
      return;
    }

    const d = data.slice(1).filter(row => {
      if (r.filterCol) {
        return r.f.test(row[r.filterCol].toString());;
      } else {
        return r.f.test(row.toString());
      }
    });

    ncol = Math.round(ncol) || 0;
    nrow = Math.round(nrow) || 0;

    r.row = Math.max(0, Math.min(d.length - r.rows, nrow));

    const s = d .slice(r.row)
                .map(row => '<tr><td>' + row.slice(r.col, r.col + r.cols).join('<td>'))
                .slice(0, r.rows)
                .join('');

    $(`.${r.cName} tbody`).html(s);

    $(`div.${r.cName} > div > div`).height(d.length * r.rowHeight);

    if (!scrolling) {
      // alert(r.rowHeight);
      const ht = d.length * r.rowHeight;
      const height = $(`.${r.cName} > div`).height();
      const st = (ht - height) / ((d.length - r.rows) / r.row);

      moving = true;
      $(`.${r.cName} > div`).scrollTop(st);
      setTimeout(() => moving = false, 10);
    }

    if (r.update) {
      r.update(r.row + 1);
    }
  } // moveto

  r.moveto = moveto;

  r.filter = (filter, col, exact) => {
    if (filter instanceof RegExp) {
      r.f = filter;
    } else if (exact) {
      r.f = new RegExp(`^${filter}$`, 'i');
    } else {
      r.f = new RegExp(filter, 'i');
    }
    r.filterCol = col;
    moveto(0, 0);
  } // filter

  /* NO GOOD
    r.filtered = (callback) => {
      const f = {...r};

      f.raw = [r.raw[0]];
      f.flds = r.flds;

      r.forEach(rec => {
        if (callback(rec)) {
          f.raw.push(rec);
        }
      });
      
      f.recordCount = f.raw.length - 1;

      return f;
    } // filtered
  */

  r.HTMLTable = (options = {}) => {
    let widths = [];
    let tableWidth;
    let style = '';

    r.cName = `table${randBetween(100, 999)}`;

    if (options.rows || options.cols) {
      r.rows = options.rows || data.length;
      r.cols = options.cols || flds.length;

      if (!options.fixed) {
        flds.forEach((_, i) => {
          widths[i] = 0;
          data.forEach(row => {
            widths[i] = Math.max(widths[i], row[i].length * 0.65);
          });
        });

        tableWidth = widths.reduce((a, b) => a + b) + 'em';
      }

      setTimeout(() => {  // can't delegate???
        r.rowHeight = $(`div.${r.cName} tbody tr`).height();

        $('body').prepend(`
          <style>
            ${widths.map((width, i) => `.${r.cName} tr > th:nth-child(${i + 1}) {width: ${width}em;}`).join('\n')}

            div.${r.cName} {
              position: relative;
              display: inline-block;
              width: auto;
            }

            div.${r.cName} > div {  /* scroller */
              position: absolute;
              height: 100%;
              width: 20px;
              left: calc(100% - 3px);
              overflow-y: ${options.rows ? 'scroll' : ''};
              overflow-x: ${options.cols ? 'scroll' : ''};
            }

            div.${r.cName} > div > div {
              height: ${data.length * r.rowHeight}px;
            }
          </style>
        `);

        $(`.${r.cName} > div`).scroll(function() {
          const ht = data.length * r.rowHeight;
          const height = $(`.${r.cName} > div`).height();
          const row = $(this).scrollTop() / (ht - height) * (data.length - r.rows)

          moveto(r.col, row, true);
        });

        $(`.${r.cName}`).on('mousewheel', (e) => {
          if (e.wheelDelta / 120 > 0) {
            moveto(r.col, r.row - r.rows);
          } else {
            moveto(r.col, r.row + r.rows);
          }
          e.preventDefault();
        });
      }, 100);

      $(document).keydown((e) => {
        if (!/INPUT|SELECT|TEXTAREA/.test(document.activeElement.tagName)) {
          switch (e.key) {
            case 'ArrowDown' : moveto(r.col, r.row + 1);
                               e.preventDefault();
                               break;
            case 'PageDown'  : moveto(r.col, r.row + r.rows);
                               e.preventDefault();
                               break;
            case 'ArrowUp'   : moveto(r.col, r.row - 1);
                                e.preventDefault();
                                break;
            case 'PageUp'    : moveto(r.col, r.row - r.rows);
                               e.preventDefault();
                               break;
            case 'End'       : if (e.ctrlKey) {
                                 moveto(r.col, Infinity);
                                 e.preventDefault();
                               }
                               break;
            case 'Home'      : if (e.ctrlKey) {
                                 moveto(r.col, 0);
                                 e.preventDefault();
                               }
                               break;
          }
        }
      });
    }

    if (!r.recordCount) {
      return '<b style="color: red;">Not found.</b>';
    } else {
      return `
        ${style}
        <div ${options.rows || options.cols ? `class="scroller ${r.cName}"` : ''}>
          <div>
            <div></div>
          </div>
          <table
            ${tableWidth ? `style="table-layout: fixed; width: ${tableWidth};"` : ''};"
          >
            <thead>
              <tr style="background:#def;"><th>${flds.slice(0, r.cols).join('<th>')}
            </thead>
            <tbody>${data.slice(1, 1 + r.rows).map(row => '<tr><td>' + row.slice(0, r.cols).join('<td>')).join('')}</tbody>
          </table>
        </div>
      `;
    }
  } // HTMLTable

  r.recordCount = r.raw.length - 1;

  r.fldCount = flds.length;
  r.flds = flds;

  r.forEach = (callback) => {
    for (let i = 1; i <= r.recordCount; i++) {
      callback(r.record(i), i);
    }
    return r;
  } // forEach

  r.data = (fld, col, value) =>  {
    const data = [];

    r.forEach(rec => {
      if (rec[fld] !== '' && rec[fld] !== undefined) {
        if (!col || rec[col] == value) {
          data.push(rec[fld]);
        }
      }
    });
    
    return data;
  } // data

  r.numbers = (fld) =>  {
    const data = [];

    r.forEach(rec => {
      data.push(+rec[fld]);
    });
    
    return data;
  } // numbers

  r.fields = fld => typeof fld == 'number' ? r.raw[0][fld] : r.raw[0];

  r.record = n => {
    if (!r.raw[n]) {
      return null;
    }

    for (let i = 0; i < r.fldCount; i++) {
      r.raw[n][r.flds[i]] = r.raw[n][i];
    }
  
    return r.raw[n];
  } // record

  r.find = (parm1, lookup) => {
    let result;
    let found;

    r.forEach((rec, idx) => {
      if (!found && rec[parm1] == lookup) {
        found = true;
        result = r.record(idx);
      }
    });

    return result;
  } // find

  r.toString = () => {
    try {
      return r.record(1, 0);
    } catch(ee) {
      return 'ERROR';
    }
  } // toString

  return r;
} // View

const PQ = (s, parms = {}, fnc, options) => {
  const pg = document.location.search.includes('pg=true');

  $.ajax({
    url: '/_nr_query',
    method: 'POST',
    data: {
      query: s,
      parms: JSON.stringify(parms),
      fieldNames: true,
      pg: pg,
      secure: localStorage.getItem('secure')
    },
    success: (data) => {
      let V;

      if (/^ERROR/.test(data)) {
        V = {
          errorMessage: data
        }
      } else {
        data = data.split('^^').slice(0, -1).map(d => d.split('|'));

        V = View(data[0], data);
      }

      fnc && fnc(V);
    }
  });
} // PQ

const dateFormat = function () {  // http://blog.stevenlevithan.com/archives/date-time-format, with edits
  let token = /d{1,4}|m{1,4}|yy(?:yy)?|([HhMsTt])\1?|[LloSZ]|"[^"]*"|'[^']*'/g;
  let timezone = /\b(?:[PMCEA][SDP]T|(?:Pacific|Mountain|Central|Eastern|Atlantic) (?:Standard|Daylight|Prevailing) Time|(?:GMT|UTC)(?:[-+]\d{4})?)\b/g;
  let timezoneClip = /[^-+\dA-Z]/g;
  let pad = (val, len = 2) => (val + '').padStart(len, '0');

  const i18n = {
    dayNames: [
      'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat',
      'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'
    ],
    monthNames: [
      'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
      'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'
    ]
  };

  return function (date, mask) {
    if (arguments.length == 1 && Object.prototype.toString.call(date) == "[object String]" && !/\d/.test(date)) {
      mask = date;
      date = undefined;
    }

    date = date ? new Date(date) : new Date;

    mask = mask || 'ddd mmm dd yyyy HH:MM:ss';

    const d = date.getDate();
    const D = date.getDay();
    const m = date.getMonth();
    const y = date.getFullYear();
    const H = date.getHours();
    const M = date.getMinutes();
    const s = date.getSeconds();
    const L = date.getMilliseconds();
    const o = date.getTimezoneOffset();
    const flags = {
      d:    d,
      dd:   pad(d),
      ddd:  i18n.dayNames[D],
      dddd: i18n.dayNames[D + 7],
      m:    m + 1,
      mm:   pad(m + 1),
      mmm:  i18n.monthNames[m],
      mmmm: i18n.monthNames[m + 12],
      yy:   String(y).slice(2),
      yyyy: y,
      h:    H % 12 || 12,
      hh:   pad(H % 12 || 12),
      H:    H,
      HH:   pad(H),
      M:    M,
      MM:   pad(M),
      s:    s,
      ss:   pad(s),
      l:    pad(L, 3),
      L:    pad(L > 99 ? Math.round(L / 10) : L),
      t:    H < 12 ? 'a'  : 'p',
      tt:   H < 12 ? 'am' : 'pm',
      T:    H < 12 ? 'A'  : 'P',
      TT:   H < 12 ? 'AM' : 'PM',
      Z:    (String(date).match(timezone) || ['']).pop().replace(timezoneClip, ''),
      o:    (o > 0 ? '-' : '+') + pad(Math.floor(Math.abs(o) / 60) * 100 + Math.abs(o) % 60, 4),
      S:    ['th', 'st', 'nd', 'rd'][d % 10 > 3 ? 0 : (d % 100 - d % 10 != 10) * d % 10]
    };

    return mask.replace(token, $0 => $0 in flags ? flags[$0] : $0.slice(1, $0.length - 1));
  };
}();

const linearRegression = (y, x) => { // http://trentrichardson.com/2010/04/06/compute-linear-regressions-in-javascript/
  let n = y.length;
  let sum_x = 0;
  let sum_y = 0;
  let sum_xy = 0;
  let sum_xx = 0;
  let sum_yy = 0;
  let slope;
  let i;
  
  for (i = 0; i < y.length; i++) {
    sum_x  += +x[i];
    sum_y  += +y[i];
    sum_xy += (x[i] * y[i]);
    sum_xx += (x[i] * x[i]);
    sum_yy += (y[i] * y[i]);
  } 

  slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);

  return {
    slope: slope, 
    intercept: (sum_y - slope * sum_x) / n,
    r2: Math.pow((n * sum_xy - sum_x * sum_y)/Math.sqrt((n * sum_xx - sum_x * sum_x) * (n * sum_yy - sum_y * sum_y)), 2)
  }
} // linearRegression

const Excel = (html, fname = 'Data', options) => {
  const f = $('#XFRM');

  options = {
    ...{
      orientation : 'portrait',
      repeatRows  : '$1:$1',
      worksheet   : 'Sheet1'
    },
    ...options
  };

  // console.log(options);

  if (!f.length) {
    $('body').append(`
      <form id="XFRM" method="post" action="_nr_excel" style="display:none">
        <textarea name="HTML" id="HTML"></textarea>
        <input name="Filename" id="Filename">
      </form>
    `);
  }

  $('body').append(`<div id="EXCEL">${html}</div>`);

  $('#EXCEL img').remove();
  
  $('#Filename').val(fname + '.xls');

  const styles = {};

  let stylen = 0;

  let styleSheet = `
    @page {
      mso-page-orientation:${options.orientation};
      margin: .4in .3in .4in .3in;
      mso-footer-margin: .25in;
      mso-footer-data: "&R&P of &N";
    }

    br {
      mso-data-placement: same-cell;
    }

    th {
      vertical-align: bottom;
    }
  `;

  $('#EXCEL *').each((_, el) => {
    const s = [];
    const row = $(el).closest('tr').index();
    const inbody = $(el).closest('thead').length;

    'width|white-space|color|font-family|font-size|font-weight|zpadding-left|zpadding-right|zpadding-top|zpadding-bottom|zmargin|background-color|border-left|border-right|border-top|border-bottom|zborder-collapse|text-align'.split('|').forEach(style => {
      const c = $(el).css(style);

      if (c) {
        if (/SUP|SUB/.test(el.tagName) && /font-size|vertical-align/.test(style)) {
          console.log(style);
          return;
        }

        if (style == 'width') {
          if (inbody && row == 0 && el.colSpan == 1 && parseInt(c)) {
            s.push('width:' + Math.round(parseInt(c) * 1.2) + 'px');
          }
        } else if (/border/.test(style)) {
          s.push(
            style + ':' + 
            c.replace(/((\d)+px)/, (_, __, c1) => c1 / 2 + 'pt')
          )
        } else {
          s.push(style + ':' + c);
        }
      }
    });

    const j = s.join(';');

    if (!styles[j]) {
      stylen++;
      
      styleSheet += `
        .s${stylen} {
          ${j};
        }
      `;
      
      styles[j] = stylen;
    }

    $(el).data('class', `s${styles[j]}`)
  });

  $('#EXCEL .hidden').remove();

  $('#EXCEL *').each(function() {
    const cname = $(this).data('class');
    const val = $(this).data('value');

    if (isFinite(val)) {
      this.textContent = val;
    } else if (val && this.tagName == 'TD') {
      this.textContent = `="${this.textContent}"`;
    }
    this.className = cname;
  });
  // console.log(styleSheet);  throw '';
  // console.log($('#EXCEL').html());
  // console.log(styleSheet);

  $('#EXCEL *').html((_, html) => html.replace(/\u2013/g, '-'));  // &nbsp;
  // $('#EXCEL span').remove();

  $('#HTML').val(`\uFEFF
    <html xmlns:o='urn:schemas-microsoft-com:office:office'
      xmlns:x='urn:schemas-microsoft-com:office:excel'
      xmlns='http://www.w3.org/TR/REC-html40'>
      
      <style>
        ${styleSheet}
      </style>
      
      <!--[if gte mso 9]><xml>
        <x:ExcelWorkbook>
          <x:ExcelWorksheets>
            <x:ExcelWorksheet>
              <x:Name>${options.worksheet}</x:Name>
              <x:WorksheetOptions>
                <x:DoNotDisplayGridlines/>
                <x:Print>
                  <x:ValidPrinterInfo/>
                </x:Print>
              </x:WorksheetOptions>
            </x:ExcelWorksheet>
          </x:ExcelWorksheets>
        </x:ExcelWorkbook>
     
        <x:ExcelName>
          <x:Name>Print_Titles</x:Name>
          <x:SheetIndex>1</x:SheetIndex>
          <x:Formula>=${options.worksheet}!${options.repeatRows}</x:Formula>
        </x:ExcelName>
      </xml><![endif]-->
      
      ${$('#EXCEL').html()}
    </html>
  `);

  $('#EXCEL').remove();
  // console.log($('#HTML').val()); throw '';

  // const html = $('#HTML').val();
  // window.open('data:application/vnd.ms-excel;base64,' + $.base64.encode(html));

  $('#XFRM').submit();
} // Excel

const separate = (s) => {
  const l = [];
  let labs = s.trim().replace(/[\s]*\-[\s]*/g, '-');

  labs = labs.replace(/,/g, ' ').split(/[\s]+/g);

  labs.forEach(lab => {
    const ll = lab.split('-');
    
    if (ll.length == 1) {
      ll[1] = ll[0];
    }

    for (let k = +ll[0]; k <= +ll[1]; k++) {
      if (!l.includes(+k)) {
        l.push(k);
      }
    }
  });
  
  l.sort((a, b) => a - b);

  return l;
} // separate

const simplify = (input, prefix = '', delimiter = ' ') => {
  let result = '';
 
  separate(input.toString())
    .forEach((val, idx, arr) => {
      if (+val !== arr[idx + 1] - 1) {
        result += prefix + val + delimiter;
      } else if (+val !== +arr[idx - 1] + 1) {
        result += prefix + val + '-';
      }
    }
  );

  return result.slice(0, -delimiter.length);
} //simplify

var getFeeSchedule = (fnc, options) => {  // var so it can be redefined
  $.getScript('/scripts/FeeSchedule.js?' + Math.random(), function() {
    getFeeSchedule(fnc, options);
  });
} // getFeeSchedule

var dbRequested = (table, labs, fnc) => {  // var so it can be redefined
  $.getScript('/scripts/FeeSchedule.js?' + Math.random(), function() {
    getFeeSchedule(function() {
      dbRequested(table, labs, fnc);
    });
  });
} // dbRequested

var email = (function() {
  const emopts = [];
  let emqtimer;

  return (opts) => {
    const style = (el) => {
      const s = [];
      let styles = 'zwidth|box-sizing|display|color|font|font-family|font-size|padding-left|padding-right|padding-top|padding-bottom|margin|background-color|border-left|border-right|border-top|border-bottom|border-collapse|text-align'.split('|');

      $.each(styles, function(_, style) {  // !!!
        var c = $(el).css(style);

        if (style === 'width' && el.offsetWidth) {
          s.push('width:' + el.offsetWidth + 'px');
        } else {
          s.push(style + ':' + c);
        }
//        console.log(c + ' ' + el.offsetWidth);
      });
      $(el).attr('style', s.join(';')); // + ';' + $(el).attr('style'));
    } // style

    const emq = () => {
      if (emopts.length) {
        let opts = emopts.shift();
        
        $('#em_')[0].action += '1';
        $('#em_from').val(opts.from);
        $('#em_to').val(opts.to);
        $('#em_cc').val(opts.cc);
        $('#em_bcc').val(opts.bcc);
        $('#em_attachment').val(opts.attachment);
        $('#em_subject').val(opts.subject);

        if (opts.id) {
          alert('Contact Rick: email opts.id');
        } else if (typeof opts.message === 'object') {
          style(opts.message);
          $(opts.message).find('*').each(function() {
            style(this);  
          });
          $('#em_message').val($(opts.message)[0].outerHTML);
        } else {
          $('#em_message').val(opts.message);
        }

        $('#' + opts.statusid).html(opts.statusmsg);

        $('#em_')[0].submit();
        
        clearTimeout(emqtimer);
        emqtimer = setTimeout(emq, 100);
      }
    } // emq

    if (!$('#em_').length) {
      const s = `
        <div style="display: none">
          <form id="em_" method="post" target="em_frame" action="/_nr_email2?">
            <input id="em_from"    name="em_from">
            <input id="em_to"      name="em_to">
            <input id="em_cc"      name="em_cc">
            <input id="em_bcc"     name="em_bcc">
            <input id="em_subject" name="em_subject">
            <input id="em_attachment" name="em_attachment">
            <textarea id="em_message" name="em_message"></textarea>
            <input type="submit" id="em_submit">
          </form>
          <iframe name="em_frame"></iframe>
        </div>`;
      
      $('<div>').html(s).appendTo('body');

      $('iframe[name=em_frame]').load(function() {
        if (opts.callback) {
          opts.callback();  
        }
      });
    }

    emopts.push(opts);
    clearTimeout(emqtimer);
    emqtimer = setTimeout(emq, 100);
  } 
})(); // email

const randBetween = (min, max) => Math.floor(Math.random() * (max - min + 1) + min);

const unusedCSS = () => {
  fetch('default.js')
    .then(response => response.text())
    .then(js => {
      [...document.styleSheets].forEach(sheet => {
        if (sheet.href && sheet.href.includes(location.origin + location.pathname)) {
          const ruleList = [...document.styleSheets].pop().cssRules;

          for (let rule of ruleList) {
            const sel = (rule.selectorText || '').split(/, */);

            sel.forEach(sel => {
              if (sel && !/:/.test(sel) && !$(sel).length) {
                let found = false;

                sel.match(/\w+/).forEach(s => {
                  const re = new RegExp('\\b' + s + '\\b');
                  
                  if (re.test(js)) {
                    found = true;
                  }
                });
    
                if (found) {
                  console.log(`Unused CSS, but may be in JavaScript: ${sel}`)
                } else {
                  console.log(`Unused CSS: ${sel}`)
                }
              }
            });
          }
        }
      });
    }
  );
} // unusedCSS

const html2pdf = (viewer, html, fname, options = {}, callback) => {
  let styles = [];

  [...document.styleSheets].forEach(ss => {
    [...ss.rules].forEach(rule => {
      styles.push(rule.cssText);
    });
  });

  styles = `
    <style>
      ${styles.join('\n')}
    </style>
  `;

  if (!html) {
    $(viewer).hide();
    html = styles + $('body').html();
  } else {
    html = styles + html;
  }
// console.log(options)
  $.post(
    '_nr_html2pdf',
    {
      html: html,
      fname: fname,
      options: JSON.stringify(options)
    },
    fname => {
      console.log(fname);
      $(viewer).attr('src', fname + '?' + Math.random()).show();
      
      $(viewer).focus() // not working

      if (callback) {
        callback();
      }
    }
  )
} // html2pdf

const readFile = (fname, callback) => {
  $.get(`_nr_readfile?fn=${fname}`, callback);
} // readFile

const copyFile = (fname1, fname2, callback) => {
  $.get(`_nr_copyfile?fn1=${fname1}&fn2=${fname2}`, callback);
} // readFile

const debug = (view) => {
  if (view && view.errorMessage) {
    console.log(view.errorMessage);
    alert('View console');
    return true;
  }
} // debug

const isInhouse = (fnc) => {
  $('body').hide();
  $.get('/_nr_secure', function(secure) {
    if (secure) {
      fnc();
    } else {
      $('body').html('Cannot run offsite');
    }
    $('body').show();
  });
} // isInhouse
