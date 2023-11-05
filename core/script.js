(function() {
    jQuery.ajaxSetup({async:false});
    DWREngine._async = false;
    jQuery("#solapastab0").click();
    const SL = jQuery("#capaEtapaEducativaContent select");
    const get_data = () => {
        let arr=[];
        SL.find("option:selected").each((x, e)=> {
            const v = e.value.trim();
            if (v == "" || v[0] == "-") return;
            const s = jQuery(e).closest("select");
            arr.push({
                "name": s.attr("name").trim(),
                "value": v,
                "txt": e.textContent.replace(/\s+/g, " ").trim(),
                "disabled": s.is(":disabled")
            })
        })
        //while (arr.length>0 && arr[arr.length-1].disabled) arr.pop();
        return arr;
    }
    const get_last = () => {
        let last = null;
        SL.filter(":enabled").find("option:selected").each((x, e) => {
            const nxt = jQuery(e).next("option");
            if (nxt.length>0) last = nxt.eq(0);
        });
        return last;
    }
    const obj = {};
    while (true) {
        const lst = get_last();
        if (lst == null) return obj;
        lst.prop('selected', true).closest("select").change();
        get_data().forEach((val, i, arr)=>{
            let aux = obj;
            for (let c = 0; c < i; c++) {
                aux = aux[arr[c].name][arr[c].value];
            }
            if (aux[val.name] == null) aux[val.name]={};
            if (aux[val.name][val.value] == null) aux[val.name][val.value]={};
            aux[val.name][val.value]['_'] = val.txt;
        })
    }
})();