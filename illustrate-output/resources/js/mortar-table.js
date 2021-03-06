(function ($, window, document) {
    "use strict";
    
    var allInstances = [];
    
    var pluginName = "mortarTableExpandableCell",
        defaults = {
            previewSelector : '.mortar-table-expandable-cell-preview',
            contentSelector : '.mortar-table-expandable-cell-content',
            expandingSelector : '.mortar-table-expandable-cell-container',
            expandedContainerStyle : {
                'width'     : 250,
                'height'    : 180
            },
            expandedPreviewStyle : {
                'opacity'   : '0'
            },
            expandedContentStyle : {
                'opacity'   : '1'
            },
            expandInward : true,
            globalBlocking : true 
        };
    
    function MortarTableExpandableCell(element, options) {
        this.element = element;
        this.options = $.extend({}, defaults, options);
        
        
        var item = $(this.element).find(this.options.expandingSelector)
            , preview = $(item).find(this.options.previewSelector)
            , content = $(item).find(this.options.contentSelector)
            , _this = this;
        
        
        item.stylestack('enable');
        preview.stylestack('enable');
        content.stylestack('enable');
        
        
        $(this.element).click( function(event) {
            _this.open();
        });
        $(this.element).find(this.options.contentSelector).click( function(event) {
            event.stopPropagation();
        });      
        
        allInstances.push(this);              
    }
        
    
    MortarTableExpandableCell.prototype.close = function () {
        if (!$(this.element).find(this.options.expandingSelector).hasClass('active')) {
            return;
        } 
        
        $(this.element).find(this.options.expandingSelector).stylestack('pop', function () {
            $(this).removeClass('active'); 
        });
        $(this.element).find(this.options.previewSelector).stylestack('pop');
        $(this.element).find(this.options.contentSelector).stylestack('pop');
        
        $(this.element).trigger('MortarTableExpandableCell.Closed');
    };
    
    MortarTableExpandableCell.prototype.open = function() {
        var item = $(this.element).find(this.options.expandingSelector)
            , preview = $(item).find(this.options.previewSelector)
            , content = $(item).find(this.options.contentSelector)
            , containerStyles = _getStyleBasedOnPosition.call(this)
            , _this = this;
        
        if(!item.hasClass('active')) {
            item.css(containerStyles['containerStyle']);
            
            // This is a hack to let the page reflow
            setTimeout(function() {
              item.addClass('active');
              content.css('display', 'block');
              
                item.transition(containerStyles['containerAnimatedStyle'], function() {
                    _this.opening = false;
                });
                preview.transition(_this.options.expandedPreviewStyle);
                content.transition(_this.options.expandedContentStyle);
                
                item.css({ 'z-index' : '1000' });
            }, 0);
        }
    }
    
    var _getStyleBasedOnPosition = function() {
        
        var containerStyle = {}
            , containerAnimatedStyle = {}
            , container = $(this.element).find(this.options.expandingSelector)
            , width = container.outerWidth()
            , height = container.outerHeight(); 
        
        containerAnimatedStyle['top'] = '50%';
        containerAnimatedStyle['left'] = '50%';
        containerAnimatedStyle['margin-left'] = -(this.options.expandedContainerStyle.width / 2);
        containerAnimatedStyle['margin-top'] = -(this.options.expandedContainerStyle.height / 2);
                
        if( this.options.expandInward ) {
            var firstInRow = $(this.element).is(':first-child')
                , lastInRow = $(this.element).is(':last-child')
                , firstRow = $(this.element).parent().is(':first-child')
                , lastRow = $(this.element).parent().is(':last-child');
            
            
            if( firstInRow && !lastInRow ) {
                containerAnimatedStyle['left'] = 0;
                containerAnimatedStyle['margin-left'] = 0;
                console.log('First in Row');
            }
            if( lastInRow && !firstInRow ) {
                containerStyle['left'] = 'auto';
                containerStyle['right'] = 0;
                containerAnimatedStyle['left'] = undefined;
                containerAnimatedStyle['margin-left'] = 0;
                console.log('Last in Row');
            }
            if( firstRow ) {
                //containerAnimatedStyle['top'] = 0;
                //containerAnimatedStyle['margin-top'] = 0;
                console.log('First Row');
            }
            if( lastRow && !firstRow ) {
                //containerStyle['top'] = 'auto';
                //containerAnimatedStyle['top'] = undefined;
                //containerAnimatedStyle['bottom'] = 0;
                //containerAnimatedStyle['margin-bottom'] = 0;
                console.log('Last Row');
            }
        }
        
        
        containerAnimatedStyle = $.extend({}, containerAnimatedStyle, this.options.expandedContainerStyle);
        
        return { 'containerStyle' : containerStyle, 'containerAnimatedStyle' : containerAnimatedStyle };
    };
    
    var _closeAllInstances = function() {
        console.log("CLOSE ALL INSTANCES");
      for(var i = 0; i < allInstances.length; i++) {
        allInstances[i].close();
      }
    };
    
    $('body').click( _closeAllInstances );
    $(document).keyup(function(e) {
      if (e.keyCode == 27) { _closeAllInstances() }   // esc
    });
        
    $.fn[pluginName] = function (option) {
        return this.each(function () {
            var $this = $(this)
                , data = $this.data('plugin_' + pluginName)
                , options = typeof option == 'object' && option
                , action = typeof option == 'string' && option
            if (!data) $.data(this, 'plugin_' + pluginName, (data = new MortarTableExpandableCell(this, options)));
            if (action) data[action]();
        
        });
    };
        
    
})(jQuery, window, document);